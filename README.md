# Administração e Propriedades de Cluster com Apache Cassandra 5

Author: Prof. Barbosa<br>
Contact: infobarbosa@gmail.com<br>
Github: [infobarbosa](https://github.com/infobarbosa)

No laboratório [cassandra-widecolumn-demo](https://github.com/infobarbosa/cassandra-widecolumn-demo), o foco é o modelo de dados wide-column e a modelagem orientada a consultas em um único nó. Aquilo que lá precisou ficar no papel — **replicação física entre servidores, escolha do coordinator, níveis de consistência sob falha, hinted handoff, read repair e anti-entropia com `nodetool repair`** — é o assunto deste laboratório, agora sobre um **cluster real de 3 nós Apache Cassandra 5**.

Os comandos são copy-paste no terminal do laboratório, na ordem em que aparecem.

---

## O que você vai saber fazer ao final

- Inspecionar a topologia física e lógica de um cluster sem mestre (*peer-to-peer*), distinguindo o que cada nó sabe sobre si (`system.local`) e sobre os vizinhos (`system.peers_v2`).
- Explicar o papel dos **seed nodes**, do protocolo **Gossip** (`nodetool gossipinfo`) e do **Snitch** (`GossipingPropertyFileSnitch`) na distribuição de réplicas entre racks.
- Ler o **ring de tokens** (`nodetool status`, `nodetool ring`, `nodetool describering`) e verificar como os **vnodes** (`num_tokens: 16`) dividem o espaço do `Murmur3Partitioner`.
- Comparar na prática fatores de replicação **`RF=1`**, **`RF=2`** e **`RF=3`**, localizando em quais nós e arquivos de disco (`SSTables`) cada partição mora (`nodetool getendpoints`).
- Observar qualquer nó atuando como **coordinator** de uma consulta (`TRACING ON`), mesmo quando ele não guarda cópia daquela partição.
- Demonstrar na prática a equação de consistência forte (**$R + W > RF$**), provocando `UnavailableException` e testando `ONE`, `TWO`, `QUORUM` e `ALL` enquanto nós são desligados (`node-stop`) e religados (`node-start`).
- Observar e medir os três mecanismos de convergência e autocura do Cassandra:
  1. **Hinted Handoff** (`nodetool statushandoff`, arquivos em `/var/lib/cassandra/nodeN/hints`);
  2. **Read Repair** (provocando uma réplica defasada e vendo uma leitura em `QUORUM` consertá-la em tempo real);
  3. **Anti-Entropia Ativa** (`nodetool repair` com árvores de Merkle) e a relação crítica entre `DELETE`, **tombstones** e `gc_grace_seconds`.
- Operar o ciclo de vida de nós com `nodetool` (`drain`, `tpstats`, `netstats`, `compactionstats`, `tablestats`) e distinguir parada limpa (`node-stop`) de queda abrupta (`node-kill` com *commitlog replay*).

---

## Pré-requisitos

- Acesso ao **AWS Academy** (o ambiente é uma instância EC2 criada por um script, logo a seguir).
- Um navegador. Nada precisa ser instalado: o `cqlsh`, o `nodetool`, os utilitários de controle dos nós (`node-start`, `node-stop`, `node-kill`, `nodetool1/2/3`) já vêm prontos no ambiente.

Fora da aula, o mesmo ambiente roda em qualquer máquina com **Docker** e cerca de **3 GB de RAM** livres — veja o [Apêndice D](#d-execução-local).

---

## Índice

[O ambiente](#o-ambiente)

**Parte 1 — Topologia Peer-to-Peer, Gossip e o Anel de Tokens**

1. [Um cluster sem mestre](#1-um-cluster-sem-mestre)
2. [O anel de tokens (Ring) e Vnodes](#2-o-anel-de-tokens-ring-e-vnodes)

**Parte 2 — Replicação Física e Posicionamento no Disco**

3. [Fator de replicação na prática: RF=1, RF=2 e RF=3](#3-fator-de-replicação-na-prática-rf1-rf2-e-rf3)

**Parte 3 — Coordenação, Níveis de Consistência e Tolerância a Falhas**

4. [Qualquer nó é Coordinator](#4-qualquer-nó-é-coordinator)
5. [Níveis de consistência sob falha ($R + W > RF$)](#5-níveis-de-consistência-sob-falha-r--w--rf)

**Parte 4 — Autocura e Anti-Entropia**

6. [Hinted Handoff: quando a falha é breve](#6-hinted-handoff-quando-a-falha-é-breve)
7. [Read Repair: consertando réplicas na leitura](#7-read-repair-consertando-réplicas-na-leitura)
8. [Nodetool Repair, Tombstones e Dados Zumbis](#8-nodetool-repair-tombstones-e-dados-zumbis)

**Parte 5 — Operação e Manutenção do Cluster**

9. [Operação no dia a dia: Drain, Crash Recovery e Telemetria](#9-operação-no-dia-a-dia-drain-crash-recovery-e-telemetria)

**Apêndices**

- [A. Referência rápida de comandos do cluster](#a-referência-rápida-de-comandos-do-cluster)
- [B. Encerrando o ambiente](#b-encerrando-o-ambiente)
- [C. Troubleshooting](#c-troubleshooting)
- [D. Execução local](#d-execução-local)

---

## O ambiente

O laboratório roda em uma instância **AWS EC2** (`t3.large`) criada automaticamente a partir do **AWS CloudShell**. A instância executa um container com o **code-server** (Visual Studio Code no navegador) e um **cluster de 3 nós Apache Cassandra 5.0** (`127.0.0.1`, `127.0.0.2` e `127.0.0.3`), distribuídos em três racks (`RAC1`, `RAC2` e `RAC3`) do `datacenter1`, com as keyspaces `infobarbank` e `infobarshop` carregadas com fator de replicação **3**.

> **ATENÇÃO:**
> 1. **Região:** utilize sempre a região **Norte da Virgínia (`us-east-1`)** no Console AWS.
> 2. **Custos:** a instância EC2 e o volume EBS consomem créditos do seu laboratório AWS Academy. Ao encerrar a aula, garanta que a instância esteja parada (**Stopped**) ou aguarde o desligamento automático da sessão.
> 3. **Não recrie a máquina para continuar o trabalho:** cada execução do `launch-lab.sh` cria uma instância **nova e vazia**, com data e hora no nome. Se a sua instância já existir, siga as instruções de [Retomando o laboratório](#retomando-o-laboratório).

### Passo 1: Abrir o AWS CloudShell

1. Faça login no Console da AWS através do seu painel do AWS Academy.
2. Certifique-se de que a região selecionada no canto superior direito é **N. Virginia (`us-east-1`)**.
3. No topo do console, clique no ícone do **AWS CloudShell** (`>_`) ao lado da barra de pesquisa.
4. Aguarde o terminal do CloudShell carregar.

### Passo 2: Executar o script de provisionamento

No terminal do CloudShell, execute:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/cassandra-cluster-administration/main/launch-lab.sh | bash

```

O script realiza de forma automatizada:

- Descoberta da VPC padrão e de uma subnet pública.
- Configuração do Security Group `lab-cassandra-sg`, liberando as portas `8080` (IDE) e `22` (SSH).
- Lançamento de uma nova instância EC2 `lab-cassandra-cluster-AAAA-MM-DD_HH_MI_SS` (`t3.large`, Ubuntu 24.04, 20 GB gp3).
- Instalação do Docker e inicialização do container `cassandra-cluster-lab` (code-server + cluster de 3 nós Cassandra).
- Exibição do IP público e do link de acesso.

### Passo 3: Acessar a IDE

1. Abra no navegador o link exibido ao final do script: `http://<IP-PUBLICO>:8080`.
2. Caso a página não responda, **aguarde de 2 a 4 minutos** para que o Docker conclua o download da imagem e atualize o navegador.
3. O code-server abre diretamente na IDE, sem solicitar senha.
4. Abra um terminal: menu superior (três linhas horizontais) > **Terminal** > **New Terminal**.

> **Atenção!** Todos os comandos deste laboratório são executados no **terminal do code-server**. O CloudShell só serve para criar a instância.

### Retomando o laboratório

Se a sessão do AWS Academy expirar, a instância será parada. **Não execute o `launch-lab.sh` novamente:** ele criaria uma instância nova.

1. No Console AWS (`us-east-1`), acesse **EC2** > **Instances**.
2. Selecione a sua instância **`lab-cassandra-cluster-<data e hora>`** e clique em **Instance state** > **Start instance**.
3. Aguarde o status **Running** e copie o novo **Public IPv4 address**.
4. Acesse `http://<NOVO_IP_PUBLICO>:8080`.

### O que acontece no primeiro boot

No Cassandra, dois nós não podem fazer *bootstrap* (entrar no anel) simultaneamente. Por isso, o container sobe os 3 nós **em sequência**: inicia o `node1` (seed), aguarda ele ficar `UN` (*Up/Normal*), inicia o `node2`, aguarda `UN`, inicia o `node3`, aguarda `UN` e, ao final, carrega as keyspaces `infobarbank` e `infobarshop` com **`RF=3`**.

Todo esse processo leva cerca de **3 minutos** no primeiro boot e roda em segundo plano (a IDE abre antes de terminar). Acompanhe pelo log:

```sh
cat /var/log/lab/carga.log

```

```
[node1] Iniciado em background (IP 127.0.0.1, JMX 7199, log: /var/log/lab/cassandra-node1.log).
[carga] node1 (127.0.0.1) pronto (UN).
[node2] Iniciado em background (IP 127.0.0.2, JMX 7200, log: /var/log/lab/cassandra-node2.log).
[carga] node2 (127.0.0.2) pronto (UN).
[node3] Iniciado em background (IP 127.0.0.3, JMX 7201, log: /var/log/lab/cassandra-node3.log).
[carga] node3 (127.0.0.3) pronto (UN).
[carga] Cluster de 3 nos pronto:
[...]
Competencia da carga: 2026-11
Carregando infobarbank com RF=3 (log: /var/log/lab/carga-infobarbank.log)...
Carregando infobarshop com RF=3 (log: /var/log/lab/carga-infobarshop.log)...
infobarbank: ok
infobarshop: ok
Keyspaces prontas.
[carga] Iniciando o app infobarBank (log: /var/log/lab/app.log).
```

Se o arquivo ainda não tiver chegado em `Keyspaces prontas.`, aguarde alguns segundos e repita o comando.

### Verificando o ambiente

Assim que a carga terminar, confira a integridade do cluster:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/cassandra-cluster-administration/main/scripts/check.sh | bash

```

```
Ferramentas
  ✓ cqlsh
  ✓ nodetool
  ✓ nodetool1
  ✓ nodetool2
  ✓ nodetool3
  ✓ node-start
  ✓ node-stop
  ✓ node-kill
  ✓ sstabledump

Carga inicial do cluster
  ✓ bootstrap dos 3 nós e carga do primeiro boot concluídos

Nós do cluster (datacenter1)
  ✓ node1 (127.0.0.1, RAC1): UN (Up/Normal)
  ✓ node2 (127.0.0.2, RAC2): UN (Up/Normal)
  ✓ node3 (127.0.0.3, RAC3): UN (Up/Normal)
  ✓ Cassandra versão 5.0.9

Keyspaces e Replicação
  ✓ infobarbank: 12 tabelas (NetworkTopologyStrategy, datacenter1 = 3)
  ✓ infobarshop: 9 tabelas (NetworkTopologyStrategy, datacenter1 = 3)

Dados
  ✓ infobarbank.cliente: 50 clientes
  ✓ infobarshop.pedido_detalhe: 12004 pedidos

Resultado
  Cluster pronto para o laboratório.
```

### Mapa dos 3 nós e ferramentas do laboratório

Neste ambiente, os 3 nós rodam em processos isolados dentro do mesmo container, permitindo inspecionar configurações, logs e arquivos de dados dos 3 servidores lado a lado:

| Nó | Endereço IP (CQL `:9042` / Gossip `:7000`) | Rack | Porta JMX | Atalho `nodetool` | Configuração | Dados (`SSTables`, `hints`, `commitlog`) |
|---|---|---|---|---|---|---|
| **`node1`** *(Seed)* | `127.0.0.1` | `RAC1` | `7199` | `nodetool1` (ou `nodetool`) | `/etc/cassandra/node1` | `/var/lib/cassandra/node1` |
| **`node2`** | `127.0.0.2` | `RAC2` | `7200` | `nodetool2` | `/etc/cassandra/node2` | `/var/lib/cassandra/node2` |
| **`node3`** | `127.0.0.3` | `RAC3` | `7201` | `nodetool3` | `/etc/cassandra/node3` | `/var/lib/cassandra/node3` |

Para simular operações de manutenção e quedas de servidor durante a aula, você tem três comandos utilitários no `PATH`:

- `node-stop <1|2|3>` — descarrega as memtables em disco (`nodetool drain`) e para o nó de forma limpa;
- `node-kill <1|2|3>` — mata o processo do nó abruptamente (`kill -9`, sem `drain`), simulando queda de energia ou perda de máquina;
- `node-start <1|2|3>` — inicia novamente o nó em segundo plano.

---

# Parte 1 — Topologia Peer-to-Peer, Gossip e o Anel de Tokens

## 1. Um cluster sem mestre

No Cassandra não existe nó primário, *master* ou *name node*: todos os nós executam exatamente o mesmo binário e têm o mesmo papel. Qualquer nó pode receber conexões de clientes e coordenar leituras ou escritas.

![Escrita com RF=3: a aplicação envia a requisição ao Node 6, que atua como coordinator e repassa às três réplicas, Nodes 3, 4 e 5](diagramas/02-coordinator-replicas.svg)

### 1.a. O ponto de vista do operador: `nodetool status`

Peça ao `node1` o estado atual do cluster:

```sh
# 1.a.1
nodetool status

```

```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.3  130.17 KiB  16      100.0%            4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
UN  127.0.0.2  80.06 KiB   16      100.0%            3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      100.0%            8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
```

Observe cada coluna:

- **`UN` (*Up / Normal*):** a primeira letra indica se o nó está vivo (`U` = *Up*, `D` = *Down*); a segunda indica o estado dele no anel (`N` = *Normal*, `J` = *Joining*, `L` = *Leaving*, `M` = *Moving*).
- **`Address`:** os três nós (`127.0.0.1`, `127.0.0.2`, `127.0.0.3`).
- **`Tokens: 16`:** cada nó ocupa 16 posições (*vnodes*) no anel.
- **`Owns (effective): 100.0%`:** como as keyspaces de usuário existentes (`infobarbank` e `infobarshop`) têm fator de replicação 3 num cluster de 3 nós, cada nó guarda 100% das chaves dessas keyspaces. Veremos na Seção 3 como esse percentual muda quando o fator de replicação é menor que o número de nós.
- **`Rack`:** cada nó está em um rack distinto (`RAC1`, `RAC2`, `RAC3`) dentro do `datacenter1`.

O comando `nodetool` conversa com um nó via **JMX** (por padrão na porta `7199`, que é a do `node1`). Os atalhos `nodetool1`, `nodetool2` e `nodetool3` apontam para as portas `7199`, `7200` e `7201`. Confira que o `node2` enxerga exatamente o mesmo anel:

```sh
# 1.a.2
nodetool2 status

```

### 1.b. O que cada nó sabe sobre si e sobre os vizinhos (`system.local` e `system.peers_v2`)

Cada nó mantém no seu próprio disco uma keyspace interna chamada `system`. Nela, duas tabelas descrevem a topologia sob o ponto de vista **daquele nó**:

- `system.local`: uma única linha — quem **eu** sou;
- `system.peers_v2` (e `system.peers`): quem são os **outros** nós do cluster.

Pergunte ao **`node1` (`127.0.0.1`)**:

```sh
# 1.b.1
cqlsh 127.0.0.1 -e "
SELECT broadcast_address, data_center, rack, release_version FROM system.local;
SELECT peer, data_center, rack, schema_version FROM system.peers_v2;"

```

```
 broadcast_address | data_center | rack | release_version
-------------------+-------------+------+-----------------
         127.0.0.1 | datacenter1 | RAC1 |           5.0.9

(1 rows)

 peer      | data_center | rack | schema_version
-----------+-------------+------+--------------------------------------
 127.0.0.2 | datacenter1 | RAC2 | 22083a76-7c65-3f62-9755-68029f1a6f35
 127.0.0.3 | datacenter1 | RAC3 | 22083a76-7c65-3f62-9755-68029f1a6f35

(2 rows)
```

Agora faça **exatamente a mesma pergunta**, mas conectando no **`node2` (`127.0.0.2`)**:

```sh
# 1.b.2
cqlsh 127.0.0.2 -e "
SELECT broadcast_address, data_center, rack, release_version FROM system.local;
SELECT peer, data_center, rack, schema_version FROM system.peers_v2;"

```

```
 broadcast_address | data_center | rack | release_version
-------------------+-------------+------+-----------------
         127.0.0.2 | datacenter1 | RAC2 |           5.0.9

(1 rows)

 peer      | data_center | rack | schema_version
-----------+-------------+------+--------------------------------------
 127.0.0.1 | datacenter1 | RAC1 | 22083a76-7c65-3f62-9755-68029f1a6f35
 127.0.0.3 | datacenter1 | RAC3 | 22083a76-7c65-3f62-9755-68029f1a6f35

(2 rows)
```

Repare em dois detalhes importantes para a administração:

1. No `node2`, `system.local` é o `127.0.0.2` (`RAC2`), e o `127.0.0.1` aparece como mais um vizinho em `system.peers_v2`.
2. A coluna **`schema_version`** tem exatamente o mesmo UUID nos nós. Sempre que você executa um `CREATE TABLE` ou `ALTER TABLE` em qualquer nó, a definição se propaga pelo cluster e todos os nós convergem para o mesmo `schema_version`.

Confira a visão consolidada do cluster com `nodetool describecluster`:

```sh
# 1.b.3
nodetool describecluster

```

```
Cluster Information:
	Name: infobarbosainc
	Snitch: org.apache.cassandra.locator.GossipingPropertyFileSnitch
	DynamicEndPointSnitch: enabled
	Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	Schema versions:
		22083a76-7c65-3f62-9755-68029f1a6f35: [127.0.0.1:7000, 127.0.0.2:7000, 127.0.0.3:7000]
```

> Em operação real, antes de aplicar uma nova alteração de esquema (`DDL`), o administrador sempre confere se há apenas **uma** linha em `Schema versions`. Se dois nós discordarem da versão do esquema (*schema disagreement*), não se deve rodar novo `DDL` até que eles convirjam.

### 1.c. Como os nós se encontram: Seeds, Snitch e Configs

De onde vieram o nome do cluster, os endereços, os racks e o ponto de encontro inicial? Dos arquivos de configuração de cada nó em `/etc/cassandra/node{1,2,3}`. Compare-os:

```sh
# 1.c.1
grep -E "^(cluster_name|endpoint_snitch|listen_address|rpc_address)|seeds:" \
    /etc/cassandra/node1/cassandra.yaml \
    /etc/cassandra/node2/cassandra.yaml \
    /etc/cassandra/node3/cassandra.yaml

```

```
/etc/cassandra/node1/cassandra.yaml:cluster_name: 'infobarbosainc'
/etc/cassandra/node1/cassandra.yaml:      - seeds: "127.0.0.1:7000"
/etc/cassandra/node1/cassandra.yaml:listen_address: 127.0.0.1
/etc/cassandra/node1/cassandra.yaml:rpc_address: 127.0.0.1
/etc/cassandra/node1/cassandra.yaml:endpoint_snitch: GossipingPropertyFileSnitch
[...]
```

E os arquivos de topologia (`cassandra-rackdc.properties`), lidos pelo `GossipingPropertyFileSnitch`:

```sh
# 1.c.2
head -n 5 /etc/cassandra/node{1,2,3}/cassandra-rackdc.properties

```

```
==> /etc/cassandra/node1/cassandra-rackdc.properties <==
dc=datacenter1
rack=RAC1

==> /etc/cassandra/node2/cassandra-rackdc.properties <==
dc=datacenter1
rack=RAC2

==> /etc/cassandra/node3/cassandra-rackdc.properties <==
dc=datacenter1
rack=RAC3
```

Dois conceitos que costumam gerar confusão:

- **O que é um Seed Node (`seeds: "127.0.0.1:7000"`)?** O seed **não é um mestre**. Ele é apenas a agenda telefônica inicial que um nó novo consulta na primeira vez que entra no cluster para descobrir quem faz parte do anel. Depois que o nó entrou no anel e gravou os vizinhos em `system.peers_v2`, o seed é um nó igual a qualquer outro — pode inclusive cair sem parar o cluster.
- **O que é o Snitch (`GossipingPropertyFileSnitch`)?** É o componente que informa ao Cassandra em qual **datacenter** e em qual **rack** cada nó está. Ao replicar dados com `NetworkTopologyStrategy`, o Cassandra usa essa informação para espalhar as cópias em **racks diferentes** (para que a queda de um rack inteiro — um switch ou régua de energia — não derrube todas as cópias de um dado).

### 1.d. O protocolo Gossip (`nodetool gossipinfo`)

A cada segundo, cada nó escolhe de um a três vizinhos e troca mensagens na porta `7000` dizendo o que sabe sobre si e sobre os outros. Esse protocolo epidêmico se chama **Gossip**. Veja o estado do Gossip diretamente:

```sh
# 1.d.1
nodetool gossipinfo

```

```
/127.0.0.1
  generation:1793656782
  heartbeat:245
  STATUS:22:NORMAL,-1010539016797727098
  LOAD:209:1395432.0
  SCHEMA:37:22083a76-7c65-3f62-9755-68029f1a6f35
  DC:12:datacenter1
  RACK:14:RAC1
  RELEASE_VERSION:6:5.0.9
  HOST_ID:4:8e9b590e-48ab-43be-bd42-ad2b9fb1295b
  TOKENS:21:<hidden>
/127.0.0.2
  generation:1793656821
  heartbeat:206
  STATUS:21:NORMAL,-1047400249635601538
  [...]
  DC:11:datacenter1
  RACK:13:RAC2
  [...]
/127.0.0.3
  generation:1793656859
  heartbeat:168
  STATUS:21:NORMAL,-1128945112039481920
  [...]
  DC:11:datacenter1
  RACK:13:RAC3
```

Duas linhas contam a história de vida de cada nó:

- **`generation`:** o timestamp (em segundos desde 1970) de quando aquele nó **iniciou**. Só muda quando o processo do Cassandra naquele nó é reiniciado.
- **`heartbeat`:** um contador que sobe a cada segundo enquanto o nó está vivo. Se você rodar `nodetool gossipinfo` novamente daqui a 5 segundos, verá que o `generation` dos três nós continuou igual, mas o `heartbeat` de todos subiu em ~5. É observando o ritmo do `heartbeat` que um nó percebe que outro caiu (*Phi Accrual Failure Detector*).

---

## 2. O anel de tokens (Ring) e Vnodes

Como vimos no laboratório anterior, o Cassandra aplica a função de hash `Murmur3Partitioner` sobre a **partition key** de cada linha, gerando um inteiro de 64 bits entre $-2^{63}$ e $2^{63}-1$: o **token**.

![A partition key id_cliente passa pela função hash Murmur3; a Marivalda gera o token 225 e cai no Node 3, faixa 200..299; a Jucilene gera o token 523 e cai no Node 6, faixa 500..599](diagramas/04-ring-token.svg)

### 2.a. Os 48 tokens do anel (`nodetool ring`)

No ambiente de 1 nó, o `nodetool ring` mostrava 16 linhas, todas de `127.0.0.1`. Agora cada um dos 3 nós sorteou 16 tokens (`num_tokens: 16`), dividindo o anel em **48 faixas intercaladas**:

```sh
# 2.a.1
nodetool ring | head -n 30

```

```
Datacenter: datacenter1
==========
Address         Rack        Status State   Load            Owns                Token
                                                                               9145612938471928374
127.0.0.2       RAC2        Up     Normal  80.06 KiB       100.00%             -9091827364519283746
127.0.0.1       RAC1        Up     Normal  1.33 MiB        100.00%             -8812736451928374612
127.0.0.3       RAC3        Up     Normal  130.17 KiB      100.00%             -8519283746192837461
127.0.0.2       RAC2        Up     Normal  80.06 KiB       100.00%             -8192837461928374612
[...]
```

Conte quantas posições cada nó ocupa no anel:

```sh
# 2.a.2
nodetool ring | grep -E "^127\.0\.0\.[123]" | awk '{print $1, $2}' | sort | uniq -c

```

```
     16 127.0.0.1 RAC1
     16 127.0.0.2 RAC2
     16 127.0.0.3 RAC3
```

Por que **16 tokens pequenos por nó (*vnodes*)** em vez de 1 token grande por nó?
- Com 1 token por nó, adicionar um 4º servidor obrigaria a recalcular manualmente os tokens dos outros 3 ou copiar toda a carga de um único vizinho.
- Com 16 *vnodes* espalhados pelo anel, quando um 4º nó entra no cluster ele assume pequenas fatias de **todos** os nós existentes ao mesmo tempo, distribuindo o tráfego de rede (*streaming*) e o balanceamento de forma uniforme.

---

# Parte 2 — Replicação Física e Posicionamento no Disco

## 3. Fator de replicação na prática: RF=1, RF=2 e RF=3

O **fator de replicação (*Replication Factor*, RF)** é definido na criação da `KEYSPACE`. Ele responde a uma pergunta simples: **em quantos nós diferentes cada partição desta keyspace será gravada?**

![A aplicação envia o INSERT da Marivalda ao Node 6, que atua como coordinator; ele calcula o token 225, que cai na faixa 200..299 do Node 3, e repassa a escrita às três réplicas, Nodes 3, 4 e 5, porque o RF é 3; os Nodes 4 e 5 guardam cópia da faixa 200..299](diagramas/04d-token-coordinator-rf.svg)

Vamos criar três keyspaces de experimento (`lab_rf1`, `lab_rf2` e `lab_rf3`), uma com cada fator de replicação, e inserir a mesma cliente — a Marivalda (`2b162060`) — nas três.

### 3.a. Criando `lab_rf1`, `lab_rf2` e `lab_rf3`

```sh
# 3.a.1
cqlsh 127.0.0.1 -e "
CREATE KEYSPACE IF NOT EXISTS lab_rf1
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE KEYSPACE IF NOT EXISTS lab_rf2
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2};

CREATE KEYSPACE IF NOT EXISTS lab_rf3
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

CREATE TABLE IF NOT EXISTS lab_rf1.cliente (id_cliente text PRIMARY KEY, nome text);
CREATE TABLE IF NOT EXISTS lab_rf2.cliente (id_cliente text PRIMARY KEY, nome text);
CREATE TABLE IF NOT EXISTS lab_rf3.cliente (id_cliente text PRIMARY KEY, nome text);

INSERT INTO lab_rf1.cliente (id_cliente, nome) VALUES ('2b162060', 'MARIVALDA');
INSERT INTO lab_rf2.cliente (id_cliente, nome) VALUES ('2b162060', 'MARIVALDA');
INSERT INTO lab_rf3.cliente (id_cliente, nome) VALUES ('2b162060', 'MARIVALDA');
"

```

### 3.b. Como o `Owns (effective)` muda conforme a Keyspace

Quando você roda `nodetool status` passando o nome de uma keyspace, a coluna `Owns (effective)` calcula a fatia do anel que cada nó armazena **para aquela keyspace**. Compare as três:

```sh
# 3.b.1
echo "=== RF = 1 ===" && nodetool status lab_rf1 | grep -E "^(UN|--)"
echo "=== RF = 2 ===" && nodetool status lab_rf2 | grep -E "^(UN|--)"
echo "=== RF = 3 ===" && nodetool status lab_rf3 | grep -E "^(UN|--)"

```

```
=== RF = 1 ===
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.3  130.17 KiB  16      38.0%             4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
UN  127.0.0.2  80.06 KiB   16      29.6%             3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      32.4%             8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
=== RF = 2 ===
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.3  130.17 KiB  16      76.0%             4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
UN  127.0.0.2  80.06 KiB   16      59.3%             3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      64.7%             8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
=== RF = 3 ===
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.3  130.17 KiB  16      100.0%            4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
UN  127.0.0.2  80.06 KiB   16      100.0%            3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      100.0%            8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
```

Some os percentuais de `Owns (effective)` em cada caso:
- Em **`lab_rf1`**, a soma dos 3 nós dá **100%** (~33% em cada nó): cada partição existe **uma única vez** no cluster. Se um nó cair, ~1/3 dos dados fica inacessível.
- Em **`lab_rf2`**, a soma dá **200%** (~67% em cada nó): cada partição mora em **dois nós** de racks diferentes.
- Em **`lab_rf3`**, a soma dá **300%** (**100%** em cada nó): cada partição tem uma cópia em `RAC1`, uma em `RAC2` e uma em `RAC3`.

### 3.c. Descobrindo em quais nós mora uma chave (`nodetool getendpoints`)

O comando `nodetool getendpoints <keyspace> <tabela> <chave>` aplica o `Murmur3Partitioner` na chave, percorre o anel de tokens e devolve exatamente quais nós guardam as réplicas daquela partição.

Consulte onde mora a Marivalda (`2b162060`) nas três keyspaces:

```sh
# 3.c.1
echo "=== Réplicas de 2b162060 em lab_rf1 (RF=1) ==="
nodetool getendpoints lab_rf1 cliente 2b162060

echo "=== Réplicas de 2b162060 em lab_rf2 (RF=2) ==="
nodetool getendpoints lab_rf2 cliente 2b162060

echo "=== Réplicas de 2b162060 em lab_rf3 (RF=3) ==="
nodetool getendpoints lab_rf3 cliente 2b162060

```

```
=== Réplicas de 2b162060 em lab_rf1 (RF=1) ===
127.0.0.2
=== Réplicas de 2b162060 em lab_rf2 (RF=2) ===
127.0.0.2
127.0.0.1
=== Réplicas de 2b162060 em lab_rf3 (RF=3) ===
127.0.0.2
127.0.0.1
127.0.0.3
```

> Os IPs exatos de `RF=1` e `RF=2` dependem dos tokens sorteados no seu container, mas repare na progressão: o primeiro IP de `RF=2` é sempre o dono primário da faixa (`RF=1`), o segundo é o próximo nó no sentido horário do anel em **outro rack**, e o terceiro completa os três racks!

### 3.d. A prova física no disco dos 3 nós (`SSTables`)

Isso não é abstração lógica: se `lab_rf1` tem `RF=1`, apenas **um** dos três diretórios `/var/lib/cassandra/node{1,2,3}` terá arquivo `Data.db` para `lab_rf1.cliente`.

Force a descida das memtables para o disco nos 3 nós (`flush`) e liste os arquivos `*-Data.db` gerados em cada nó:

```sh
# 3.d.1
nodetool1 flush
nodetool2 flush
nodetool3 flush

echo "=== Arquivos Data.db de lab_rf1 (RF=1) ==="
ls -1 /var/lib/cassandra/node{1,2,3}/data/lab_rf1/cliente-*/*-Data.db 2>/dev/null

echo "=== Arquivos Data.db de lab_rf2 (RF=2) ==="
ls -1 /var/lib/cassandra/node{1,2,3}/data/lab_rf2/cliente-*/*-Data.db 2>/dev/null

echo "=== Arquivos Data.db de lab_rf3 (RF=3) ==="
ls -1 /var/lib/cassandra/node{1,2,3}/data/lab_rf3/cliente-*/*-Data.db 2>/dev/null

```

```
=== Arquivos Data.db de lab_rf1 (RF=1) ===
/var/lib/cassandra/node2/data/lab_rf1/cliente-4a12.../nb-1-big-Data.db
=== Arquivos Data.db de lab_rf2 (RF=2) ===
/var/lib/cassandra/node1/data/lab_rf2/cliente-8c34.../nb-1-big-Data.db
/var/lib/cassandra/node2/data/lab_rf2/cliente-8c34.../nb-1-big-Data.db
=== Arquivos Data.db de lab_rf3 (RF=3) ===
/var/lib/cassandra/node1/data/lab_rf3/cliente-9f01.../nb-1-big-Data.db
/var/lib/cassandra/node2/data/lab_rf3/cliente-9f01.../nb-1-big-Data.db
/var/lib/cassandra/node3/data/lab_rf3/cliente-9f01.../nb-1-big-Data.db
```

Compare a saída do `ls` acima com a saída do `nodetool getendpoints` da seção 3.c: **são exatamente os mesmos nós.** Em `lab_rf1`, os outros dois nós conhecem o esquema da tabela (os diretórios existem), mas estão **vazios** — não há nenhum byte da Marivalda neles.

---

# Parte 3 — Coordenação, Níveis de Consistência e Tolerância a Falhas

## 4. Qualquer nó é Coordinator

Em `lab_rf1`, a linha da Marivalda (`2b162060`) existe em **apenas um nó** do cluster. O que acontece se a aplicação se conectar a um dos **outros dois nós** (que não têm o dado no disco) e pedir essa linha?

Descubra qual nó **não** tem a réplica de `lab_rf1` e faça a consulta nele com `TRACING ON`:

```sh
# 4.a.1
REPLICA=$(nodetool getendpoints lab_rf1 cliente 2b162060 | head -1)
# Escolhe um nó diferente da réplica para atuar apenas como Coordinator
if [ "$REPLICA" = "127.0.0.1" ]; then COORD="127.0.0.3"; else COORD="127.0.0.1"; fi
echo "Réplica está em: $REPLICA | Conectando no Coordinator: $COORD"

cqlsh "$COORD" -e "
TRACING ON;
SELECT * FROM lab_rf1.cliente WHERE id_cliente = '2b162060';
"

```

```
Réplica está em: 127.0.0.2 | Conectando no Coordinator: 127.0.0.1

 id_cliente | nome
------------+-----------
   2b162060 | MARIVALDA

(1 rows)

Tracing session: ...

 activity                                                                          | timestamp    | source    | source_elapsed | client
-----------------------------------------------------------------------------------+--------------+-----------+----------------+-----------
                                                                Execute CQL3 query | 16:30:12.101 | 127.0.0.1 |              0 | 127.0.0.1
                                         Parsing SELECT * FROM lab_rf1.cliente ... | 16:30:12.101 | 127.0.0.1 |             92 | 127.0.0.1
                                                               Preparing statement | 16:30:12.101 | 127.0.0.1 |            215 | 127.0.0.1
                                             Sending READ message to /127.0.0.2:7000 | 16:30:12.102 | 127.0.0.1 |            610 | 127.0.0.1
                                      REQUEST_RESPONSE message received from /127.0.0.2:7000 | 16:30:12.104 | 127.0.0.1 |           2480 | 127.0.0.1
                                        Processing response from /127.0.0.2:7000   | 16:30:12.104 | 127.0.0.1 |           2590 | 127.0.0.1
                                                  Executing single-partition query | 16:30:12.102 | 127.0.0.2 |            180 | 127.0.0.1
                                                      Acquiring sstable references | 16:30:12.102 | 127.0.0.2 |            230 | 127.0.0.1
                                                       Read 1 live rows and 0 tombstone cells | 16:30:12.103 | 127.0.0.2 |            890 | 127.0.0.1
                                            Enqueuing response to /127.0.0.1:7000  | 16:30:12.103 | 127.0.0.2 |           1020 | 127.0.0.1
                                                                  Request complete | 16:30:12.104 | 127.0.0.1 |           2812 | 127.0.0.1
```

Olhe a coluna `source` do rastro acima:
1. `127.0.0.1` recebeu a consulta do cliente (`Execute CQL3 query`), calculou o token de `'2b162060'` e viu que ele não tinha a partição.
2. `127.0.0.1` enviou uma mensagem interna na porta `7000` para o dono do dado (`Sending READ message to /127.0.0.2:7000`).
3. `127.0.0.2` leu a SSTable no disco dele (`Read 1 live rows`) e devolveu a resposta ao `127.0.0.1`.
4. `127.0.0.1` entregou o resultado ao `cqlsh`.

> É por isso que drivers de produção usam **`TokenAwarePolicy`** junto com *Prepared Statements*: sabendo a partition key no cliente, o próprio driver calcula o token e envia a requisição direto para uma das réplicas, economizando esse salto de rede intermediário.

---

## 5. Níveis de consistência sob falha ($R + W > RF$)

O **fator de replicação (`RF`)** define em quantos nós o dado **deve** ser gravado.
O **nível de consistência (*Consistency Level*, CL)** é escolhido por consulta (na leitura e na escrita) e define **quantas réplicas precisam responder antes de o coordinator dar a operação como concluída**.

Para uma keyspace com **`RF = 3`** (como a `infobarbank` e a `lab_rf3`), os níveis principais são:

| Consistency Level | Réplicas exigidas (em `RF=3`) | Quantos nós podem cair sem erro? |
|---|---|---|
| **`ONE`** / **`LOCAL_ONE`** | **1** réplica | Até **2 nós** podem cair |
| **`TWO`** | **2** réplicas | Até **1 nó** pode cair |
| **`QUORUM`** / **`LOCAL_QUORUM`** | $\lfloor RF/2 \rfloor + 1 =$ **2** réplicas | Até **1 nó** pode cair |
| **`ALL`** | **3** réplicas (todas) | **0 nós** podem cair (qualquer falha bloqueia a operação) |

### A regra da Consistência Forte ($R + W > RF$)

Se a aplicação escreve exigindo confirmação de $W$ réplicas e lê exigindo resposta de $R$ réplicas, sempre que:

$$R + W > RF$$

o conjunto de nós lidos ($R$) e o conjunto de nós escritos ($W$) **obrigatoriamente têm pelo menos 1 nó em comum** (Princípio da Casa dos Pombos). Na leitura, o coordinator compara os timestamps das $R$ respostas e vence a mais recente (*last-write-wins*).

Em um cluster com **`RF = 3`**:
- Escrever em **`QUORUM` ($W=2$)** e ler em **`QUORUM` ($R=2$)** soma $2 + 2 = 4 > 3$: entrega **consistência imediata (forte)** e ainda tolera a perda de **1 servidor** tanto na leitura quanto na escrita!
- Escrever em **`ONE` ($W=1$)** e ler em **`ONE` ($R=1$)** soma $1 + 1 = 2 \ngtr 3$: entrega máxima disponibilidade e menor latência, mas permite **consistência eventual** (você pode ler da réplica 3 um milissegundo antes de a escrita chegar nela).

Vamos testar isso derrubando servidores de verdade.

### 5.a. Cenário 1: Todos os 3 nós ativos (`UN`)

Com os 3 nós de pé, até o nível mais rigoroso (`ALL`) funciona na keyspace `infobarbank` (`RF=3`):

```sh
# 5.a.1
cqlsh 127.0.0.1 -e "
CONSISTENCY ALL;
SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
Consistency level set to ALL.

 id_cliente | apelido_cliente | saldo_disponivel
------------+-----------------+------------------
   2b162060 |       MARIVALDA |          4384.61

(1 rows)
```

### 5.b. Cenário 2: Parando 1 nó (`node-stop 3`) — 2 nós vivos, 1 nó `DN`

Pare o `node3` (`127.0.0.3`, no `RAC3`) e confira o estado do anel:

```sh
# 5.b.1
node-stop 3
nodetool1 status

```

```
[node3] Executando nodetool drain (JMX 7201)...
[node3] Parado.
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
DN  127.0.0.3  130.17 KiB  16      100.0%            4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
UN  127.0.0.2  80.06 KiB   16      100.0%            3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      100.0%            8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
```

O `node3` agora aparece como **`DN` (*Down / Normal*)**: continua fazendo parte do anel (`Normal`), mas está desligado (`Down`).

Tente agora a mesma consulta exigindo **`CONSISTENCY ALL`**:

> **Provoque este erro.**

```sh
# 5.b.2
cqlsh 127.0.0.1 -e "
CONSISTENCY ALL;
SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
Consistency level set to ALL.
NoHostAvailable: ('Unable to complete the operation against any hosts', {<Host: 127.0.0.1:9042 datacenter1>: Unavailable('Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ALL" info={\'consistency\': \'ALL\', \'required_replicas\': 3, \'alive_replicas\': 2}')})
```

Leia a mensagem do servidor:
`Cannot achieve consistency level ALL — required_replicas: 3, alive_replicas: 2`.
O coordinator nem tentou ler do disco: pelo Gossip ele já sabia que só havia 2 réplicas vivas e rejeitou a operação imediatamente.

Agora repita a leitura e faça também uma escrita usando **`CONSISTENCY QUORUM`** (exige 2 de 3):

```sh
# 5.b.3
cqlsh 127.0.0.1 -e "
CONSISTENCY QUORUM;
UPDATE infobarbank.conta_corrente
SET saldo_disponivel = 5000.00
WHERE id_cliente = '2b162060';

SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
Consistency level set to QUORUM.

 id_cliente | apelido_cliente | saldo_disponivel
------------+-----------------+------------------
   2b162060 |       MARIVALDA |             5000

(1 rows)
```

Mesmo com o `RAC3` inteiro fora do ar, o banco **continua lendo e gravando com consistência forte (`QUORUM`)**, porque `node1` (`RAC1`) e `node2` (`RAC2`) responderam!

### 5.c. Cenário 3: Parando o 2º nó (`node-stop 2`) — apenas 1 nó vivo (`127.0.0.1`)

E se cair um segundo rack? Pare o `node2`:

```sh
# 5.c.1
node-stop 2
nodetool1 status

```

```
[node2] Executando nodetool drain (JMX 7200)...
[node2] Parado.
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
DN  127.0.0.3  130.17 KiB  16      100.0%            4e25811e-f98e-40ea-9a7f-579a62ce0728  RAC3
DN  127.0.0.2  80.06 KiB   16      100.0%            3cb14774-e000-4dbf-bf41-205a9f206e8b  RAC2
UN  127.0.0.1  1.33 MiB    16      100.0%            8e9b590e-48ab-43be-bd42-ad2b9fb1295b  RAC1
```

Tente consultar com `CONSISTENCY QUORUM`:

> **Provoque este erro.**

```sh
# 5.c.2
cqlsh 127.0.0.1 -e "
CONSISTENCY QUORUM;
SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
Consistency level set to QUORUM.
NoHostAvailable: (..., Unavailable('Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level QUORUM" info={\'consistency\': \'QUORUM\', \'required_replicas\': 2, \'alive_replicas\': 1}'))
```

Agora mude para **`CONSISTENCY ONE`** (ou `LOCAL_ONE`, que é o padrão de muitas leituras de alta disponibilidade):

```sh
# 5.c.3
cqlsh 127.0.0.1 -e "
CONSISTENCY ONE;
SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
Consistency level set to ONE.

 id_cliente | apelido_cliente | saldo_disponivel
------------+-----------------+------------------
   2b162060 |       MARIVALDA |             5000

(1 rows)
```

Dois terços do cluster estão desligados e a aplicação ainda consegue ler e escrever em `CONSISTENCY ONE`!

Antes de seguir, **religue apenas o `node2`** e aguarde ele ficar `UN` (deixe o `node3` parado para a próxima seção):

```sh
# 5.c.4
node-start 2
sleep 8
nodetool1 status

```

---

# Parte 4 — Autocura e Anti-Entropia

Na seção 5.b, quando o `node3` estava desligado (`DN`), nós atualizamos o `saldo_disponivel` da Marivalda para **`5000.00`** usando `CONSISTENCY QUORUM` (gravado no `node1` e no `node2`).

Surge a pergunta fundamental de qualquer banco distribuído: **como o `node3` recebe a escrita que aconteceu enquanto ele estava fora do ar?**

O Cassandra tem **três linhas de defesa**, cada uma para um cenário:

| Mecanismo | Quando atua | Janela / Gatilho |
|---|---|---|
| **1. Hinted Handoff** | Automaticamente na **escrita** quando uma réplica está `DN` | Falhas curtas (até `max_hint_window: 3h` por padrão) |
| **2. Read Repair** | Automaticamente na **leitura** (`QUORUM`/`ALL`) quando réplicas divergem | Quando o dado é lido por alguém |
| **3. `nodetool repair`** | Operação administrativa periódica (anti-entropia ativa) | Varre todas as partições (mesmo as nunca lidas), dentro de `gc_grace_seconds` (10 dias) |

Vamos demonstrar os três mecanismos na prática.

---

## 6. Hinted Handoff: quando a falha é breve

Quando o `node1` coordenou o `UPDATE saldo_disponivel = 5000.00` na seção 5.b e viu que a réplica `127.0.0.3` estava `DN`, ele não simplesmente ignorou o `node3`: ele gravou no seu próprio disco um **hint** (um bilhete contendo a mutação perdida e o `Host ID` do `node3`) para entregar assim que o `node3` voltar ao anel.

### 6.a. Inspecionando o hint guardado no Coordinator

Com o `node3` ainda parado (`DN`), force o descarregamento do buffer de hints do `node1` para o disco e veja o arquivo em `/var/lib/cassandra/node1/hints`:

```sh
# 6.a.1
nodetool1 flushhints
ls -lh /var/lib/cassandra/node1/hints/

```

```
total 8.0K
-rw-r----- 1 cassandra cassandra 158 Nov  2 16:35 4e25811e-f98e-40ea-9a7f-579a62ce0728-1793657012345-1.hints
-rw-r----- 1 cassandra cassandra  32 Nov  2 16:35 4e25811e-f98e-40ea-9a7f-579a62ce0728-1793657012345-1.crc32
```

Repare no nome do arquivo `.hints`: o prefixo (`4e25811e-...`) é exatamente o **`Host ID` do `node3`** que aparece no `nodetool status`!

### 6.b. Religando o `node3` e vendo a entrega automática do hint

Agora religue o `node3`, aguarde ele ficar `UN` e consulte o saldo da Marivalda conectando **diretamente no `node3`** com `CONSISTENCY ONE`:

```sh
# 6.b.1
node-start 3
for _ in $(seq 1 30); do
    nodetool1 status | grep -qE "^UN[[:space:]]+127\.0\.0\.3" && break
    sleep 2
done
sleep 5
nodetool1 status

cqlsh 127.0.0.3 -e "
CONSISTENCY ONE;
SELECT id_cliente, apelido_cliente, saldo_disponivel
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
 id_cliente | apelido_cliente | saldo_disponivel
------------+-----------------+------------------
   2b162060 |       MARIVALDA |             5000

(1 rows)
```

E olhe novamente o diretório `/var/lib/cassandra/node1/hints/`:

```sh
# 6.b.2
ls -la /var/lib/cassandra/node1/hints/

```

O arquivo `.hints` **desapareceu**: assim que o Gossip avisou que o `node3` voltou para `UN`, o `node1` enviou a mutação pendente para o `node3` e apagou o bilhete!

> **Por que o Hinted Handoff não basta sozinho?** Para não encher o disco dos outros nós quando uma máquina morre de vez, o Cassandra só guarda hints por até **`max_hint_window: 3h`** (configurado no `cassandra.yaml`). Se um nó ficar fora por 4 horas, ou se o próprio coordinator cair antes de entregar o hint, a réplica volta **defasada**. É aí que entram o **Read Repair** e o **`nodetool repair`**.

---

## 7. Read Repair: consertando réplicas na leitura

Vamos simular o que acontece quando um nó perde uma escrita **sem** receber hint (por exemplo, porque ficou fora além de `max_hint_window`).

### 7.a. Provocando uma réplica defasada (*stale replica*) no `node3`

1. Desabilitamos temporariamente o Hinted Handoff nos nós 1 e 2 (`disablehandoff`);
2. Paramos o `node3` (`node-stop 3`);
3. Atualizamos o saldo da Marivalda para **`9999.99`** com `CONSISTENCY QUORUM` (gravado apenas em `node1` e `node2`, **sem gerar hint** para o `node3`);
4. Religamos o `node3` e reabilitamos o Hinted Handoff.

Execute:

```sh
# 7.a.1
nodetool1 disablehandoff
nodetool2 disablehandoff

node-stop 3

cqlsh 127.0.0.1 -e "
CONSISTENCY QUORUM;
UPDATE infobarbank.conta_corrente
SET saldo_disponivel = 9999.99
WHERE id_cliente = '2b162060';
"

node-start 3
for _ in $(seq 1 30); do
    nodetool1 status | grep -qE "^UN[[:space:]]+127\.0\.0\.3" && break
    sleep 2
done

nodetool1 enablehandoff
nodetool2 enablehandoff
echo "node3 de volta ao anel (mas sem ter recebido a escrita 9999.99)!"

```

### 7.b. Observando a divergência entre os nós (`CONSISTENCY ONE`)

Agora que o `node3` está `UN`, vamos consultar a tabela `lab_rf3.cliente` (ou criar uma linha divergente direto no `node3`)? Espere: mesmo com `CONSISTENCY ONE`, se você conectar no `node3` e pedir `infobarbank.conta_corrente`, o `node3` vai ler do seu próprio disco local!

Vamos conferir o que o **`node1`** tem no disco vs. o que o **`node3`** tem no disco (usando `read_repair_chance = 0` implícito do `CONSISTENCY ONE` local):

```sh
# 7.b.1
echo "=== Lendo no node1 (127.0.0.1) com CONSISTENCY ONE ==="
cqlsh 127.0.0.1 -e "
CONSISTENCY ONE;
SELECT id_cliente, saldo_disponivel, WRITETIME(saldo_disponivel)
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

echo "=== Lendo no node3 (127.0.0.3) com CONSISTENCY ONE ==="
cqlsh 127.0.0.3 -e "
CONSISTENCY ONE;
SELECT id_cliente, saldo_disponivel, WRITETIME(saldo_disponivel)
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
=== Lendo no node1 (127.0.0.1) com CONSISTENCY ONE ===
 id_cliente | saldo_disponivel | writetime(saldo_disponivel)
------------+------------------+-----------------------------
   2b162060 |          9999.99 |            1793657489123456

=== Lendo no node3 (127.0.0.3) com CONSISTENCY ONE ===
 id_cliente | saldo_disponivel | writetime(saldo_disponivel)
------------+------------------+-----------------------------
   2b162060 |             5000 |            1793657102345678
```

Olhe o resultado acima:
- O **`node1`** diz que o saldo da Marivalda é **`9999.99`** (com `writetime` mais novo).
- O **`node3`** diz que o saldo da Marivalda ainda é **`5000`** (com `writetime` antigo)!
- Como a leitura foi feita em `CONSISTENCY ONE` diretamente no `node3`, ele consultou apenas o seu próprio disco, não comparou com ninguém e devolveu o **dado defasado (*stale read*)**!

### 7.c. Disparando o Read Repair com uma leitura em `QUORUM` (ou `ALL`)

Agora faça uma única leitura com **`CONSISTENCY ALL`** (ou `QUORUM`) e **`TRACING ON`** para ver o coordinator detectar o conflito de `writetime` e consertar o `node3` no ato:

```sh
# 7.c.1
cqlsh 127.0.0.1 -e "
CONSISTENCY ALL;
TRACING ON;
SELECT id_cliente, saldo_disponivel, WRITETIME(saldo_disponivel)
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

Procure no rastro (`Tracing session`) as linhas de **Digest mismatch** e **Read-repair**:
1. O coordinator pede o dado a uma réplica e um *digest* (hash dos dados + timestamps) às outras.
2. Os hashes não batem (`Digest mismatch`) porque o `node3` ainda tinha `5000`.
3. O coordinator pede os dados completos às réplicas, compara os `writetime` célula a célula, devolve o vencedor (`9999.99`) para você e envia uma mutação de **Read Repair** (`Sending READ_REPAIR message to /127.0.0.3:7000`) para atualizar o `node3`!

Confirme repetindo a leitura com **`CONSISTENCY ONE`** direto no **`node3`**:

```sh
# 7.c.2
cqlsh 127.0.0.3 -e "
CONSISTENCY ONE;
SELECT id_cliente, saldo_disponivel, WRITETIME(saldo_disponivel)
FROM infobarbank.conta_corrente
WHERE id_cliente = '2b162060';
"

```

```
 id_cliente | saldo_disponivel | writetime(saldo_disponivel)
------------+------------------+-----------------------------
   2b162060 |          9999.99 |            1793657489123456
```

O `node3` foi curado pela leitura!

---

## 8. Nodetool Repair, Tombstones e Dados Zumbis

O **Read Repair** da seção 7 só conserta as partições que **alguém lê**. E quanto aos milhões de clientes que não abriram o aplicativo naquela semana?

Para sincronizar todas as faixas de tokens de uma tabela ou keyspace — inclusive as partições "frias" — existe o **`nodetool repair`**.

### 8.a. Executando o `nodetool repair`

O `nodetool repair` calcula uma **Árvore de Merkle** (uma árvore de hashes criptográficos das faixas de tokens) em cada réplica, compara as árvores pela rede e transfere (*streaming*) apenas os intervalos que divergem.

Execute um reparo completo nas nossas keyspaces de laboratório `lab_rf2` e `lab_rf3`:

```sh
# 8.a.1
nodetool1 repair --full lab_rf3

```

```
[2026-11-02 16:40:10,123] Starting repair command #1 (...), repairing keyspace lab_rf3 with repair options (parallelism: parallel, primary range: false, incremental: false, job threads: 1, ColumnFamilies: [], dataCenters: [], hosts: [], previewKind: NONE, # of ranges: 48, pull repair: false, force repair: false, optimise streams: false, ignore unreplicated keyspaces: false)
[2026-11-02 16:40:11,456] Repair session ... for range [(-8812..., -8519...]] finished (progress: 100%)
[2026-11-02 16:40:11,480] Repair completed successfully
```

### 8.b. Por que `DELETE` cria Tombstones e o perigo dos Dados Zumbis

No laboratório anterior você viu que um `DELETE` no Cassandra não apaga o dado na hora: ele grava um marcador de exclusão chamado **tombstone**, que fica no disco por **`gc_grace_seconds`** (por padrão `864000` segundos = **10 dias**).

Por que o Cassandra precisa guardar uma lápide por 10 dias em vez de apagar logo? Agora que você tem um cluster de 3 nós, o motivo fica claro:

1. Imagine que o **`node3`** está desligado (`DN`) quando a aplicação executa `DELETE FROM cliente WHERE id_cliente = 'X'`.
2. Se o `node1` e o `node2` simplesmente apagassem a linha `'X'` sem deixar rastro, quando o `node3` voltasse ao anel (com a linha `'X'` ainda no disco dele), no próximo Read Repair ou `nodetool repair` o cluster acharia que o `node1` e o `node2` "perderam" a linha `'X'` e copiaria a linha apagada do `node3` de volta para todo o cluster! A linha ressuscitaria (**dado zumbi**).
3. Com o **tombstone**, o `node1` e o `node2` guardam: *"a chave `'X'` foi apagada no timestamp $T_2$"*. Quando o `node3` volta com a linha `'X'` de timestamp $T_1 < T_2$, o tombstone vence a comparação de timestamp e propaga a exclusão para o `node3`.

> **Regra de ouro da operação de cluster Cassandra:**
> O `nodetool repair` precisa rodar com uma periodicidade menor que o `gc_grace_seconds` (10 dias), e **nenhum nó que ficou desligado por mais tempo que `gc_grace_seconds` deve ser religado diretamente no anel** sem antes ter seus dados limpos e reconstruídos (`nodetool rebuild` / *replace node*) — caso contrário, ele reintroduzirá registros apagados cujos tombstones já foram coletados!

---

# Parte 5 — Operação e Manutenção do Cluster

## 9. Operação no dia a dia: Drain, Crash Recovery e Telemetria

### 9.a. Parada limpa (`node-stop` / `drain`) vs. Queda abrupta (`node-kill` / *Commitlog Replay*)

Quando você usa `node-stop 2`, o script executa `nodetool2 drain` antes de encerrar o processo:
- O nó para de aceitar escritas;
- Descarrega todas as *memtables* da RAM para arquivos `SSTable` no disco;
- Descarta os segmentos antigos do `commitlog`. Ao subir de novo, o boot é mais rápido porque não há *commitlog* pendente.

E quando a máquina perde energia (`kill -9`) com dados ainda na *memtable* (sem `flush` e sem `drain`)? Vamos testar com `node-kill`:

1. Inserimos 3 clientes novos na `lab_rf3.cliente` (eles vão para a *memtable* e para o *commitlog* dos 3 nós, mas ainda **não** viraram `SSTable`);
2. Matamos o `node2` abruptamente com `node-kill 2` (`SIGKILL`);
3. Religamos o `node2` (`node-start 2`) e inspecionamos o log dele (`/var/log/cassandra/node2/system.log`) para ver o **Commitlog Replay** recuperando as escritas da memória perdida!

```sh
# 9.a.1
cqlsh 127.0.0.1 -e "
INSERT INTO lab_rf3.cliente (id_cliente, nome) VALUES ('c1', 'TESTE CRASH 1');
INSERT INTO lab_rf3.cliente (id_cliente, nome) VALUES ('c2', 'TESTE CRASH 2');
INSERT INTO lab_rf3.cliente (id_cliente, nome) VALUES ('c3', 'TESTE CRASH 3');
"

node-kill 2
node-start 2

for _ in $(seq 1 30); do
    nodetool1 status | grep -qE "^UN[[:space:]]+127\.0\.0\.2" && break
    sleep 2
done

grep -i "Log replay" /var/log/cassandra/node2/system.log | tail -n 5

```

```
INFO  [main] 2026-11-02 16:45:02,110 CommitLog.java:186 - Replaying /var/lib/cassandra/node2/commitlog/CommitLog-8-1793656821123.log, ...
INFO  [main] 2026-11-02 16:45:02,340 CommitLog.java:192 - Log replay complete, 3 replayed mutations
```

Olhe a última linha do log do `node2`: **`Log replay complete, 3 replayed mutations`**. Mesmo tendo sofrido um `kill -9` antes do `flush`, nenhuma escrita confirmada se perdeu porque o `commitlog` em disco foi reprocessado na subida.

### 9.b. Os comandos de diagnóstico do operador (`nodetool`)

Quatro comandos do `nodetool` formam o painel de instrumentos básico para investigar gargalos em qualquer nó do cluster:

#### 1. `nodetool info` — Resumo de memória (Heap / Off-heap), Uptime e Caches de um nó

```sh
# 9.b.1
nodetool1 info

```

```
ID                     : 8e9b590e-48ab-43be-bd42-ad2b9fb1295b
Gossip active          : true
Native Transport active: true
Load                   : 1.35 MiB
Generation No          : 1793656782
Uptime (seconds)       : 942
Heap Memory (MB)       : 198.42 / 496.00
Off Heap Memory (MB)   : 0.04
Data Center            : datacenter1
Rack                   : RAC1
Exceptions             : 0
Key Cache              : entries 68, size 5.86 KiB, capacity 24 MiB, 142 hits, 218 requests, 0.651 recent hit rate, 14400 save period in seconds
```

#### 2. `nodetool tpstats` — Filas de Threads (*Thread Pool Statistics*) e Mensagens Descartadas

Em um nó sobrecarregado, você procura duas coisas no `nodetool tpstats`: tarefas bloqueadas (`Blocked` / `All time blocked`) e mensagens descartadas por timeout (`Dropped` em `READ`, `MUTATION`, `HINT`):

```sh
# 9.b.2
nodetool1 tpstats

```

#### 3. `nodetool compactionstats` e `nodetool netstats` — Compactações e Streaming em andamento

Quando um `nodetool repair`, bootstrap ou compactação pesada está rodando, estes dois comandos mostram o progresso em tempo real:

```sh
# 9.b.3
nodetool1 compactionstats
nodetool1 netstats

```

#### 4. `nodetool tablestats` — Latência de leitura/escrita, SSTables por leitura e espaço por tabela

```sh
# 9.b.4
nodetool1 tablestats infobarbank.conta_corrente

```

### 9.c. Limpeza das keyspaces de experimento

Para deixar o cluster limpo apenas com as keyspaces originais do curso (`infobarbank` e `infobarshop`) e restaurar o saldo original da Marivalda:

```sh
# 9.c.1
cqlsh 127.0.0.1 -e "
DROP KEYSPACE IF EXISTS lab_rf1;
DROP KEYSPACE IF EXISTS lab_rf2;
DROP KEYSPACE IF EXISTS lab_rf3;
"

```

---

# Apêndices

## A. Referência rápida de comandos do cluster

### Controle dos nós no laboratório (`cluster-ctl`)
| Comando | Descrição |
|---|---|
| `node-start <1\|2\|3>` | Inicia o nó especificado em segundo plano |
| `node-stop <1\|2\|3>` | Executa `nodetool drain` no nó e encerra o processo de forma limpa |
| `node-kill <1\|2\|3>` | Mata o processo do nó abruptamente (`SIGKILL`), sem `drain` |
| `nodetool1 <comando>` | Executa `nodetool` no `node1` (`127.0.0.1`, JMX `7199`, `RAC1`) |
| `nodetool2 <comando>` | Executa `nodetool` no `node2` (`127.0.0.2`, JMX `7200`, `RAC2`) |
| `nodetool3 <comando>` | Executa `nodetool` no `node3` (`127.0.0.3`, JMX `7201`, `RAC3`) |
| `cqlsh 127.0.0.<1\|2\|3>` | Abre sessão CQL usando o nó escolhido como **Coordinator** |

### Comandos `nodetool` essenciais para administração em produção
| Comando | O que faz |
|---|---|
| `nodetool status [keyspace]` | Estado dos nós (`UN`/`DN`/`UJ`/`UL`), carga e percentual de posse (`Owns`) |
| `nodetool describecluster` | Nome do cluster, snitch, particionador e concordância de `schema_version` |
| `nodetool gossipinfo` | Estado detalhado do Gossip (`generation`, `heartbeat`, `STATUS`, `DC`, `RACK`) |
| `nodetool ring` | Lista todos os tokens (*vnodes*) do anel e seus respectivos donos |
| `nodetool getendpoints <ks> <tb> <key>` | Devolve os IPs dos nós que armazenam as réplicas daquela partition key |
| `nodetool repair [ks]` | Sincroniza réplicas comparando árvores de Merkle |
| `nodetool flush [ks]` | Força a gravação das *memtables* em arquivos `SSTable` no disco |
| `nodetool drain` | Faz `flush` de tudo, fecha o *commitlog* e para de aceitar conexões antes de desligar |
| `nodetool decommission` | Retira o nó atual do anel de forma limpa, transferindo seus dados aos vizinhos |
| `nodetool removenode <HostID>` | Remove do anel um nó morto que não pode mais rodar `decommission` |
| `nodetool cleanup [ks]` | Apaga de um nó as faixas de tokens que deixaram de pertencer a ele após a entrada de novos nós |

---

## B. Encerrando o ambiente

Ao fim de cada aula, pare a instância: no Console AWS (`us-east-1`), **EC2** > **Instances**, selecione a sua instância **`lab-cassandra-cluster-<data e hora>`** e clique em **Instance state** > **Stop instance**. Os dados são preservados, e na próxima aula basta seguir [Retomando o laboratório](#retomando-o-laboratório).

Quando o laboratório tiver terminado de vez, use **Instance state** > **Terminate instance**.

---

## C. Troubleshooting

Antes de qualquer coisa, rode o diagnóstico do cluster:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/cassandra-cluster-administration/main/scripts/check.sh | bash

```

### `nodetool: Failed to connect to '127.0.0.1:7199' - ConnectException: 'Connection refused'`

O comando `nodetool` padrão conecta na porta `7199` (que pertence ao `node1`). Se você parou o `node1` (`node-stop 1`) ou se o cluster ainda está subindo:
- Para consultar o cluster através do `node2` ou `node3`, use **`nodetool2 status`** ou **`nodetool3 status`**.
- Para religar o `node1`, execute **`node-start 1`**.

### Um nó aparece como `DN` (`Down/Normal`) quando deveria estar ativo

Você provavelmente o desligou em algum exercício anterior. Religue todos os nós que estiverem parados com:

```sh
node-start 1
node-start 2
node-start 3

```

Aguarde cerca de 10 segundos e confira com `nodetool status`.

### Recomeçar as bases do curso do zero

Se quiser restaurar `infobarbank` e `infobarshop` exatamente como vieram no primeiro boot (com todos os 3 nós `UN`):

```sh
carregar-keyspaces

```

---

## D. Execução local

Fora do horário de aula, é possível executar o mesmo ambiente em qualquer máquina com Docker. A imagem é publicada para `linux/amd64` e `linux/arm64` (inclusive Macs com Apple Silicon), e precisa de cerca de **3 GB de RAM** livres.

Baixe o `compose.yaml` numa pasta vazia e inicialize o container:

```sh
mkdir cassandra-cluster-lab && cd cassandra-cluster-lab
curl -sSO https://raw.githubusercontent.com/infobarbosa/cassandra-cluster-administration/main/compose.yaml
docker compose up -d

```

Ou, sem clonar o repositório:

```sh
docker run -d --name cassandra-cluster-lab -p 8080:8080 --stop-timeout 60 ghcr.io/infobarbosa/cassandra-cluster-lab-docker-image:latest

```

Acesse `http://localhost:8080` no navegador e acompanhe o bootstrap inicial dos 3 nós pelo terminal da IDE com `cat /var/log/lab/carga.log`.
