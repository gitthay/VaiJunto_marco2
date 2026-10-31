# VaiJunto — Consórcio de Caronas Compartilhadas 🚗💨

Sistema distribuído de caronas compartilhadas para viagens de média e longa distância, desenvolvido para a disciplina **TEC502 — Concorrência e Conectividade** (**Problema 1** e **Problema 2: Consórcio de Caronas Compartilhadas**).

O projeto implementa múltiplos servidores independentes em Go (representando empresas distintas de um consórcio regional), onde:
- **Clientes (motoristas e passageiros)** conectam-se exclusivamente ao servidor de sua própria empresa via **Sockets TCP puros** (JSON sobre TCP), com processamento ordenado por **Fila de Requisições (Worker Pool)**.
- **Servidores das empresas do consórcio** cooperam de forma **100% descentralizada via requisições REST (HTTP/JSON)** utilizando **Batimento Cardíaco (Heartbeat)**, **Busca de Grafo Federada (BFS)**, **Two-Phase Commit (2PC)** em memória com critério de precedência por **Relógio Lógico de Lamport**, recuperação automática de falhas (*Recovery* / *Presumed Abort*) e consultas/cancelamentos cruzados via REST.

---

## Principais Funcionalidades

- **Descentralização e Isolamento Estrito de Dados**: Cada servidor cria e mantém exclusivamente os seus próprios 3 arquivos originais (`caronas.json`, `reservas.json` e `usuarios.json`, além do `servidor.log`) dentro de `dados/<empresa>/` ao ser iniciado, sem banco de dados compartilhado, sem arquivo extra de transações e sem servidor central.
- **Fila de Requisições com Pool de Workers (`main.go`)**: As requisições TCP dos clientes entram em uma fila concorrente (`chan TarefaRequisicao`) consumida por um pool fixo de *workers*, evitando saturação de I/O e esgotamento de portas sob rajadas de centenas de clientes simultâneos.
- **Batimento Cardíaco (Heartbeat)**: Monitoramento contínuo entre os servidores do consórcio (`GET /api/heartbeat`). Se o servidor de uma empresa falhar/cair, os demais detectam via timeout e continuam operando normalmente para todas as rotas que não dependem da empresa offline.
- **Busca Federada de Itinerários (BFS em Grafo)**: Quando um passageiro busca uma viagem, o servidor de sua empresa consulta via REST (`GET /api/caronas`) os trechos disponíveis nas empresas parceiras online e monta o grafo unificado para encontrar roteiros combinando múltiplas empresas.
- **Reserva Atômica Distribuída (Two-Phase Commit — 2PC)**:
  - **Fase 1 (`POST /api/2pc/prepare`)**: O servidor do passageiro atua como coordenador daquela transação e solicita o bloqueio temporário dos assentos em cada empresa envolvida (em ordem determinística de URL para prevenir deadlocks distribuídos).
  - **Fase 2 (`POST /api/2pc/commit` ou `/api/2pc/abort`)**: Se todas as empresas confirmarem disponibilidade, a reserva é efetivada em todas (`COMMITTED`) e gravada no `reservas.json` do servidor do passageiro. Se qualquer empresa recusar ou falhar, todas desfazem a pré-reserva imediatamente (`ABORTED`).
- **Critério de Precedência Distribuída (Relógio Lógico de Lamport)**: Sem depender de relógios físicos sincronizados, cada transação carrega a tupla `(TimestampLamport, EmpresaOrigemID, TransacaoID)`. Em disputas simultâneas pelo último assento na fase `PREPARED`, vence de forma determinística a requisição com menor timestamp lógico (desempatando por `EmpresaOrigemID` e `TransacaoID`).
- **Tolerância a Falhas e Recuperação via REST**: Caso o coordenador falhe após a fase `PREPARED`, a rotina de *Recovery* consulta `GET /api/2pc/status` ou aplica *Presumed Abort* após o timeout, garantindo que **nenhum assento permaneça bloqueado indefinidamente**.
- **Consultas e Cancelamentos Entre Empresas 100% via REST**:
  - **Consulta de Passageiros (`GET /api/caronas/passageiros`)**: Quando um motorista consulta os passageiros da sua carona, o servidor dele consulta via REST os servidores parceiros do consórcio.
  - **Cancelamento de Reserva (`POST /api/reservas/liberar`)**: Se o passageiro cancelar uma reserva composta, seu servidor devolve os assentos locais e aciona `POST /api/reservas/liberar` nas empresas parceiras.
  - **Cancelamento de Carona (`POST /api/caronas/cancelada`)**: Se um motorista cancelar sua carona, o servidor dele desativa a carona e notifica via REST os servidores parceiros, que apagam a reserva afetada e liberam os assentos dos demais trechos daquele itinerário (inclusive sincronizando automaticamente caso algum parceiro estivesse offline no momento do cancelamento).

---

## Arquitetura do Sistema

```text
 ┌────────────────────┐             ┌────────────────────┐             ┌────────────────────┐
 │ Clientes Empresa A │             │ Clientes Empresa B │             │ Clientes Empresa C │
 │ (Motorista/Passag.)│             │ (Motorista/Passag.)│             │ (Motorista/Passag.)│
 └─────────┬──────────┘             └─────────┬──────────┘             └─────────┬──────────┘
           │ TCP (:8081)                      │ TCP (:8082)                      │ TCP (:8083)
           ▼                                  ▼                                  ▼
 ┌────────────────────┐  HTTP REST  ┌────────────────────┐  HTTP REST  ┌────────────────────┐
 │ Servidor Empresa A │◄───────────►│ Servidor Empresa B │◄───────────►│ Servidor Empresa C │
 │   REST (:9081)     │ (Heartbeat, │   REST (:9082)     │ (Heartbeat, │   REST (:9083)     │
 │  dados/empresaA/   │  BFS, 2PC)  │  dados/empresaB/   │  BFS, 2PC)  │  dados/empresaC/   │
 └────────────────────┘             └────────────────────┘             └────────────────────┘
```

### Documentação dos Protocolos
- **Protocolo TCP (Cliente ↔ Servidor Local)**: documentado em [`utils/protocolos.md`](utils/protocolos.md).
- **Especificação da API REST entre Servidores + Diagrama de Sequência 2PC**: documentada em [`utils/api_consorcio.md`](utils/api_consorcio.md).

---

## Estrutura de Arquivos

```text
redes_marco2/
├── main.go                     # Servidor (Fila de Requisições TCP + Inicialização REST Consórcio)
├── utils/
│   ├── protocolos.go           # Entidades de domínio, envelopes TCP e estrutura em memória do 2PC
│   ├── servicos.go             # Grafo/BFS, persistência isolada por instância e regras de negócio
│   ├── consorcio.go            # API REST, Heartbeat, Relógio de Lamport, 2PC e Recovery via REST
│   ├── protocolos.md           # Especificação do protocolo TCP cliente-servidor
│   └── api_consorcio.md        # Especificação completa da API REST do consórcio + Diagrama Mermaid
├── motorista/
│   └── mainmotorista.go        # Cliente CLI do motorista (conecta via TCP à sua empresa)
├── passageiro/
│   └── mainpassageiro.go       # Cliente CLI do passageiro (conecta via TCP à sua empresa)
├── simulador/
│   └── mainsimulador.go        # Simulador autônomo de passageiros sob carga (com fila de concorrência)
├── limpa/
│   └── limpa.go                # Utilitário para limpar a pasta dados/ antes de novos testes
├── testes/
│   └── concorrencia_test.go    # Suíte de 7 testes automatizados do Consórcio (2PC, Lamport, Falhas, REST)
├── Dockerfile
├── docker-compose.yml          # Orquestração das instâncias isoladas do consórcio
└── README.md
```

---

## Como Executar

### Pré-requisitos
- **Go 1.22+**
- **Docker e Docker Compose** (opcional para execução em contêineres)

---

### Opção 1: Subindo com Docker Compose

#### A) Rodando em Computadores Distintos (1 Contêiner por Máquina)
Substitua `192.168.1.10`, `192.168.1.11` e `192.168.1.12` pelos IPs reais de cada computador na rede local:

**No Linux / Bash:**
```bash
# No PC 1 (Empresa A):
IP_A="192.168.1.10" IP_B="192.168.1.11" IP_C="192.168.1.12" docker compose up --build empresaA

# No PC 2 (Empresa B):
IP_A="192.168.1.10" IP_B="192.168.1.11" IP_C="192.168.1.12" docker compose up --build empresaB

# No PC 3 (Empresa C):
IP_A="192.168.1.10" IP_B="192.168.1.11" IP_C="192.168.1.12" docker compose up --build empresaC
```

**No Windows (PowerShell):**
```powershell
# No PC 1 (Empresa A):
$env:IP_A="192.168.1.10"; $env:IP_B="192.168.1.11"; $env:IP_C="192.168.1.12"; docker compose up --build empresaA

# No PC 2 (Empresa B):
$env:IP_A="192.168.1.10"; $env:IP_B="192.168.1.11"; $env:IP_C="192.168.1.12"; docker compose up --build empresaB

# No PC 3 (Empresa C):
$env:IP_A="192.168.1.10"; $env:IP_B="192.168.1.11"; $env:IP_C="192.168.1.12"; docker compose up --build empresaC
```
*(Cada computador criará apenas a pasta `dados/` correspondente ao servidor iniciado nele).*

#### B) Rodando os 3 Servidores no Mesmo Computador
```bash
docker compose up --build
```
Portas expostas:
- **Empresa A**: TCP `localhost:8081` | HTTP REST `http://localhost:9081`
- **Empresa B**: TCP `localhost:8082` | HTTP REST `http://localhost:9082`
- **Empresa C**: TCP `localhost:8083` | HTTP REST `http://localhost:9083`

---

### Opção 2: Subindo as 3 Instâncias Localmente (Sem Docker)

Abra 3 terminais e execute um comando em cada:

```powershell
# Terminal 1 — Empresa A
go run main.go -id=empresaA -tcp=:8081 -http=:9081 -peers=http://localhost:9082,http://localhost:9083

# Terminal 2 — Empresa B
go run main.go -id=empresaB -tcp=:8082 -http=:9082 -peers=http://localhost:9081,http://localhost:9083

# Terminal 3 — Empresa C
go run main.go -id=empresaC -tcp=:8083 -http=:9083 -peers=http://localhost:9081,http://localhost:9082
```

---

### Conectando os Clientes (Motorista e Passageiro)

Os clientes conectam-se à porta TCP do servidor de sua empresa usando a flag `-server`:

```powershell
# Motorista conectando na Empresa A
go run motorista/mainmotorista.go -server 192.168.1.10:8081

# Motorista conectando na Empresa B
go run motorista/mainmotorista.go -server 192.168.1.11:8082

# Passageiro conectando na Empresa C
go run passageiro/mainpassageiro.go -server 192.168.1.12:8083
```
*(Se estiver rodando tudo na mesma máquina, use `localhost:8081`, `localhost:8082` e `localhost:8083`).*

---

### Executando o Simulador Autônomo de Passageiros sob Carga

Com as 3 instâncias em execução, rode o simulador para semear caronas inter-empresas e disparar múltiplos passageiros concorrentes buscando e reservando itinerários completos via 2PC:

```powershell
# Em 3 PCs diferentes:
go run simulador/mainsimulador.go -servers 192.168.1.10:8081,192.168.1.11:8082,192.168.1.12:8083 -clientes 300 -assentos 10

# No mesmo PC:
go run simulador/mainsimulador.go -servers localhost:8081,localhost:8082,localhost:8083 -clientes 300 -assentos 10
```

Parâmetros opcionais do simulador:
- `-servers`: lista de endereços TCP dos servidores das empresas (padrão: `localhost:8081,localhost:8082,localhost:8083`)
- `-clientes`: número de passageiros simultâneos (padrão: `300`)
- `-assentos`: número de vagas ofertadas por trecho na semeadura (padrão: `10`)
- `-fila`: tamanho máximo da fila de conexões simultâneas no disparo (padrão: `45`)
- `-seed`: `true`/`false` para publicar automaticamente as caronas antes do disparo (padrão: `true`)

### Limpando os Dados Antes de uma Nova Simulação

Para remover a pasta `dados/` e zerar o estado antes de um novo teste:
```powershell
go run limpa/limpa.go
```

---

### Executando os Testes Automatizados

A suíte em [`testes/concorrencia_test.go`](testes/concorrencia_test.go) é totalmente autossuficiente (sobe os servidores REST em pastas temporárias isoladas e injeta falhas automaticamente):

```powershell
go test ./testes -v
```

Os 7 testes validam:
1. **`Test1_HeartbeatESincronizacaoLamport`**: Monitoramento `ONLINE` via `/api/heartbeat` e avanço do Relógio Lógico de Lamport.
2. **`Test2_BuscaFederadaGrafoConsorcio`**: Montagem do grafo federado entre 3 empresas (`A + B + C`) e continuidade da busca quando uma empresa fica offline.
3. **`Test3_Concorrencia2PCDisputaUltimoAssentoInterEmpresas`**: 25 passageiros simultâneos disputando 1 vaga inter-empresas, garantindo apenas 1 confirmação e rollback atômico das outras 24.
4. **`Test4_InjecaoDeFalhaParticipanteNoMeioDaReserva`**: Queda real de servidor participante no meio do 2PC, garantindo que nenhum itinerário seja confirmado pela metade.
5. **`Test5_QuedaCoordenadorERecuperacaoAutomaticaViaREST`**: Recuperação automática via `GET /api/2pc/status` desbloqueando assento preso em `PREPARED` após falha do coordenador.
6. **`Test6_PrecedenciaRelogioLogicoLamportEDesempate`**: Resolução determinística de conflito por Relógio Lógico de Lamport e desempate alfabético por `EmpresaOrigemID` em tempos iguais.
7. **`Test7_ConsultaPassageirosECancelamentoEntreEmpresasViaREST`**: Consulta de passageiros (`GET /api/caronas/passageiros`) e cancelamento de carona em cascata (`POST /api/caronas/cancelada` e `POST /api/reservas/liberar`) 100% via requisições REST.

---

## Autor

Trabalho desenvolvido por **Thaylane da Silva** para a disciplina **TEC502 — Concorrência e Conectividade** (UEFS).
