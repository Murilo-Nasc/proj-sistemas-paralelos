
# Sistema de Troca de Mensagens Instantâneas (BBS/IRC) com ZeroMQ
 
Feito por:
 
- Murilo Gonçalves Nascimento - RA: 22.126.094-6
- Murilo Brenneken Duarte Passarelli - RA: 22.126.125-8

Projeto da disciplina de Sistemas Distribuídos e Paralelos.
 
---
 
## Sobre o projeto
 
O projeto é uma versão simplificada de um sistema de mensagens no estilo dos antigos BBS e IRC. Os usuários são bots que entram no sistema, criam e assinam canais públicos e publicam mensagens neles. Os servidores armazenam tudo em disco e, por serem vários, cooperam entre si: dividem os pedidos dos clientes, sincronizam seus relógios, escolhem um coordenador e mantêm cópias idênticas dos dados.
 
Todos os serviços rodam em containers Docker e se comunicam pela rede do Docker Compose, usando ZeroMQ. Não há interação com o usuário: ao subir o sistema, os bots começam a trocar mensagens sozinhos e os logs de cada serviço mostram tudo o que é enviado e recebido.
 
O sistema é composto por:
 
| Serviço | Função |
|---|---|
| broker | Recebe os pedidos dos clientes e os distribui entre os servidores |
| proxy | Recebe as publicações dos servidores e as entrega aos clientes inscritos em cada canal |
| referencia | Atribui um rank a cada servidor e mantém a lista dos servidores ativos |
| servidor-py-1, servidor-py-2, servidor-java-1, servidor-java-2 | Atendem os clientes e armazenam os dados |
| cliente-py-1, cliente-py-2, cliente-java-1, cliente-java-2 | Os bots |
 
---
 
## Parte 1 - Introdução e escolhas do projeto
 
### Linguagens
 
Como o grupo tem dois integrantes, usamos duas linguagens: Python e Java. Cada integrante implementou o cliente e o servidor na sua linguagem, e os dois se comunicam entre si. O broker, o proxy e o serviço de referência foram feitos em Python.
 
### Serialização
 
Usamos MessagePack. É um formato binário, mais compacto que JSON, com bibliotecas maduras em Python e Java e que não exige arquivo de esquema, o que facilitou manter as duas versões compatíveis. Todas as mensagens seguem o mesmo formato:
 
| Campo | Conteúdo |
|---|---|
| type | tipo da mensagem |
| sender | quem enviou |
| timestamp | hora de envio, em milissegundos |
| logical_clock | valor do relógio lógico de quem enviou |
| data | conteúdo, que varia conforme o tipo |
 
As respostas trazem no campo data um status (ok ou error) e, em caso de erro, um código e uma descrição.
 
### Persistência
 
Cada servidor armazena seus dados em um banco SQLite, em um arquivo próprio mantido em um volume do Docker. Escolhemos o SQLite por ser um arquivo único, funcionar da mesma forma em Python e em Java e dispensar um container extra de banco de dados. Há três tabelas principais: logins, canais e publicações.
 
---
 
## Parte 2 - Troca de mensagens e armazenamento das publicações
 
### Fluxo das mensagens
 
1. O cliente envia seus pedidos (login, criar canal, listar canais, publicar) ao broker, que os repassa a um dos servidores. O broker usa os padrões ROUTER/DEALER do ZeroMQ e alterna os servidores um a um.
2. Ao receber uma publicação, o servidor a grava no banco e a envia ao proxy, que a entrega a todos os clientes inscritos naquele canal (padrão PUB/SUB, com o proxy em XSUB/XPUB).
3. O nome do canal funciona como tópico da inscrição. Como o ZeroMQ filtra tópicos por prefixo, o cliente também confere se o nome do canal é exatamente o esperado.
### Comportamento dos bots
 
Cada bot faz login e então repete um ciclo: lista os canais, cria um novo se houver menos de 5, assina um canal se estiver inscrito em menos de 3 e publica 10 mensagens aleatórias em um canal, com 1 segundo de intervalo entre elas.
 
### Armazenamento das publicações
 
Cada publicação é gravada na tabela de publicações com o canal, o autor, o texto, o horário enviado pelo cliente, o horário em que o servidor a publicou e o servidor que a processou. Cada publicação recebe do cliente um identificador único (msg_id). Se o cliente não receber resposta e reenviar o pedido (o que faz após 3 segundos sem resposta), o servidor reconhece o identificador e não grava nem publica a mensagem duas vezes. Publicar em um canal ainda não criado é aceito.
 
Clientes e servidores exibem no log toda mensagem enviada ou recebida, com horário, tipo, remetente e conteúdo.
 
---
 
## Parte 3 - Serviço de referência e relógios
 
Nesta parte foi adicionado o serviço de referência. Ao iniciar, cada servidor solicita a ele um rank e passa a enviar um heartbeat a cada 10 requisições de clientes. Servidores que ficam 60 segundos sem enviar heartbeat são removidos da lista de ativos.
 
Também foram incluídos dois relógios:
 
- Relógio lógico: um contador mantido por todos os processos, incrementado a cada mensagem enviada e ajustado ao receber uma mensagem (assume o maior valor entre o seu e o recebido). Seu valor vai em todas as mensagens.
- Relógio físico: cada servidor mantém a hora atual com um ajuste que pode ser corrigido pela sincronização descrita na parte seguinte.
---
 
## Parte 4 - Coordenador e eleição
 
Os servidores passaram a ter um coordenador, cujo nome fica guardado em uma variável em cada servidor, e a conversar diretamente entre si (porta 5560 de cada servidor).
 
- Sincronização de relógio: a cada 15 requisições de clientes, o servidor pergunta a hora ao coordenador e ajusta o próprio relógio. Em nossa versão simplificada, o coordenador responde com a sua própria hora. O serviço de referência deixou de informar a hora.
- Eleição: se o coordenador não responde, o servidor envia um pedido de eleição aos servidores de rank maior. Se algum responde, ele assume a eleição. Se nenhum responde, o servidor se torna o coordenador e publica seu nome no tópico servers, e todos os outros atualizam a variável do coordenador. Um servidor que entra no sistema também realiza uma eleição, de modo que o de maior rank tende a ser o coordenador.
Para facilitar a visualização, o arquivo docker-compose define um desvio inicial no relógio de cada servidor (variável CLOCK_OFFSET_MS), e os logs mostram o ajuste feito em cada sincronização. A eleição pode ser observada parando o coordenador atual.
 
---
 
## Parte 5 - Método de replicação dos dados
 
### O problema
 
Como o broker distribui os pedidos em round-robin, cada servidor recebe apenas parte das mensagens. Se um servidor falhar, parte do histórico é perdida, e consultar um único servidor retorna só parte dos dados. É necessário que todos os servidores tenham todos os dados.
 
### O método escolhido
 
Adotamos uma replicação baseada em log de eventos com consistência eventual. Não há um servidor principal: todos aceitam pedidos dos clientes. Cada alteração (login, criação de canal, publicação) é registrada como um evento e enviada aos demais servidores. Além disso, os servidores comparam seus registros periodicamente e pedem uns aos outros o que estiver faltando. Por alguns instantes os servidores podem estar levemente diferentes, mas todos acabam com o mesmo conteúdo.
 
### Como foi implementado
 
- Cada evento tem uma origem (o nome do servidor mais um sufixo aleatório gerado a cada inicialização) e um número de sequência crescente. Os eventos ficam guardados em uma tabela própria de cada servidor.
- Ao registrar um evento, o servidor o publica em um tópico de replicação, pelo mesmo proxy dos canais, e todos os servidores estão inscritos nele. Quem recebe aplica o evento apenas se for o próximo da sequência daquela origem: eventos já conhecidos são ignorados e, se faltar algum, a sincronização começa na hora.
- A cada cerca de 30 segundos, cada servidor pede aos outros, por REQ/REP, os eventos que ainda não possui, informando o último número de cada origem que conhece. Isso recupera o que foi perdido por um servidor que estava fora do ar ou que entrou depois.
### Adaptações ao projeto
 
- Como não há servidor principal, a ordem dos eventos só é garantida dentro de cada origem. Isso basta porque cada publicação tem um msg_id único.
- O PUB/SUB não entrega mensagens a quem ainda não completou a inscrição e não guarda histórico, por isso a sincronização periódica foi acrescentada à publicação dos eventos.
- Os identificadores internos das tabelas podem diferir entre servidores, mas o conteúdo armazenado é o mesmo.
 
