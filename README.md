# VaiJunto — Consórcio de Caronas Compartilhadas 🚗💨

Sistema distribuído de caronas compartilhadas para viagens de média e longa distância, desenvolvido para a disciplina **TEC502 — Concorrência e Conectividade** (**Problema 1** e **Problema 2: Consórcio de Caronas Compartilhadas**).

O projeto implementa múltiplos servidores independentes em Go (representando empresas distintas de um consórcio regional), onde:
- **Clientes (motoristas e passageiros)** conectam-se exclusivamente ao servidor da sua própria empresa via **Sockets TCP puros** (JSON sobre TCP).
- **Servidores das empresas do consórcio** cooperam de forma **100% descentralizada** via **API REST (HTTP/JSON)** utilizando **Batimento Cardíaco (Heartbeat)**, **Busca de Grafo Federada (BFS)**, **Two-Phase Commit (2PC)** com critério de precedência por **Relógio Lógico de Lamport** e recuperação automática de falhas (*Recovery* / *Presumed Abort*).

---

## Principais Funcionalidades

- **Descentralização e Isolamento de Dados**: Cada empresa mantém apenas os seus próprios arquivos (`dados/<empresa>/caronas.json`, `reservas.json`, `usuarios.json`, `transacoesgerais.json` e `servidor.log`), sem banco de dados compartilhado nem servidor central.
- **Batimento Cardíaco (Heartbeat)**: Monitoramento contínuo entre os servidores do consórcio (`GET /api/heartbeat`). Se o servidor de uma empresa falhar/cair, os demais detectam imediatamente via timeout e continuam operando normalmente para todas as rotas que não dependem da empresa offline.
- **Busca Federada de Itinerários (BFS em Grafo)**: Quando um passageiro busca uma viagem, o servidor de sua empresa consulta via REST (`GET /api/caronas`) os trechos disponíveis nas empresas parceiras online e monta o grafo unificado para encontrar roteiros combinando múltiplas empresas.
- **Reserva Atômica Distribuída (Two-Phase Commit — 2PC)**:
  - **Fase 1 (`POST /api/2pc/prepare`)**: O servidor do passageiro atua como coordenador daquela transação e solicita o bloqueio temporário dos assentos em cada empresa envolvida (em ordem determinística de URL para prevenir deadlocks distribuídos).
  - **Fase 2 (`POST /api/2pc/commit` ou `/api/2pc/abort`)**: Se todas as empresas confirmarem disponibilidade, a reserva é efetivada em todas (`COMMITTED`). Se qualquer empresa recusar ou falhar, todas desfazem a pré-reserva imediatamente (`ABORTED`).
- **Critério de Precedência Distribuída (Relógio Lógico de Lamport)**: Sem depender de relógios físicos sincronizados, cada transação carrega a tupla `(TimestampLamport, EmpresaOrigemID, TransacaoID)`. Em disputas simultâneas pelo último assento na fase `PREPARED`, vence de forma determinística a requisição com menor timestamp lógico.
- **Tolerância a Falhas e Recuperação (`transacoesgerais.json`)**: Cada servidor registra apenas os IDs essenciais e referências locais das transações em andamento. Caso um servidor caia no meio de uma reserva ou o coordenador falhe, a rotina de *Recovery* consulta `GET /api/2pc/status` ou aplica *Presumed Abort* após o timeout, garantindo que **nenhum assento permaneça bloqueado indefinidamente**.
- **Cancelamentos Cruzados via Requisição REST**:
  - Se o passageiro cancelar uma reserva composta, seu servidor devolve os assentos locais e aciona `POST /api/reservas/liberar` nas empresas parceiras.
  - Se um motorista cancelar sua carona, o servidor dele devolve os assentos locais e notifica o servidor do passageiro (`POST /api/caronas/cancelada`), que remove a reserva do passageiro e libera os assentos das outras empresas envolvidas naquele itinerário.

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
├── main.go                     # Ponto de entrada do servidor (TCP clientes + HTTP REST consórcio)
├── utils/
│   ├── protocolos.go           # Entidades de domínio, envelopes TCP e modelo TransacaoGeral (2PC)
│   ├── servicos.go             # Grafo/BFS, persistência isolada por instância e regras locais
│   ├── consorcio.go            # API REST, Heartbeat, Relógio de Lamport, 2PC e Recovery
│   ├── protocolos.md           # Especificação do protocolo TCP cliente-servidor
│   └── api_consorcio.md        # Especificação completa da API REST do consórcio + Diagrama Mermaid
├── motorista/
│   └── mainmotorista.go        # Cliente CLI do motorista (conecta via TCP à sua empresa)
├── passageiro/
│   └── mainpassageiro.go       # Cliente CLI do passageiro (conecta via TCP à sua empresa)
├── simulador/
│   └── mainsimulador.go        # Simulador autônomo de passageiros sob carga (sem interface gráfica)
├── testes/
│   └── concorrencia_test.go    # Testes unitários, concorrência, carga e injeção de falhas no 2PC
├── dados/                      # Armazenamento isolado por instância de empresa (gerado em execução)
│   ├── empresaA/               # caronas.json, reservas.json, usuarios.json, transacoesgerais.json
│   ├── empresaB/
│   └── empresaC/
├── Dockerfile
├── docker-compose.yml          # Orquestração das 3 instâncias isoladas do consórcio
└── README.md
```

---

## Como Executar

### Pré-requisitos
- **Go 1.22+**
- **Docker e Docker Compose** (opcional para execução em contêineres)

---

### Opção 1: Subindo o Consórcio (3 Empresas) com Docker Compose

Para subir as 3 instâncias (`empresaA`, `empresaB` e `empresaC`) simultaneamente em contêineres isolados:

```bash
docker compose up --build
```

Portas expostas:
- **Empresa A**: TCP `localhost:8081` | HTTP REST `http://localhost:9081`
- **Empresa B**: TCP `localhost:8082` | HTTP REST `http://localhost:9082`
- **Empresa C**: TCP `localhost:8083` | HTTP REST `http://localhost:9083`

#### Executando em Computadores Distintos do Laboratório (1 Contêiner por Máquina)
Se desejar rodar cada empresa em uma máquina física diferente (ex.: IPs `192.168.1.10`, `192.168.1.11` e `192.168.1.12`):

- **Máquina 1 (`192.168.1.10` — Empresa A)**:
  ```powershell
  $env:URL_EMPRESA_A="http://192.168.1.10:9081"
  $env:PEERS_EMPRESA_A="http://192.168.1.11:9082,http://192.168.1.12:9083"
  docker compose up --build empresaA
  ```
- **Máquina 2 (`192.168.1.11` — Empresa B)**:
  ```powershell
  $env:URL_EMPRESA_B="http://192.168.1.11:9082"
  $env:PEERS_EMPRESA_B="http://192.168.1.10:9081,http://192.168.1.12:9083"
  docker compose up --build empresaB
  ```
- **Máquina 3 (`192.168.1.12` — Empresa C)**:
  ```powershell
  $env:URL_EMPRESA_C="http://192.168.1.12:9083"
  $env:PEERS_EMPRESA_C="http://192.168.1.10:9081,http://192.168.1.11:9082"
  docker compose up --build empresaC
  ```

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

Os clientes conectam-se exclusivamente à porta TCP do servidor de sua empresa usando a flag `-server`:

```powershell
# Motorista conectando na Empresa A (:8081)
go run ./motorista/mainmotorista.go -server=localhost:8081

# Motorista conectando na Empresa B (:8082)
go run ./motorista/mainmotorista.go -server=localhost:8082

# Passageiro conectando na Empresa C (:8083)
go run ./passageiro/mainpassageiro.go -server=localhost:8083
```

---

### Executando o Simulador Autônomo de Passageiros sob Carga

Com as 3 instâncias em execução (`:8081`, `:8082`, `:8083`), rode o simulador para semear caronas inter-empresas e disparar múltiplos passageiros concorrentes buscando e reservando itinerários completos:

```powershell
go run ./simulador/mainsimulador.go -servers="localhost:8081,localhost:8082,localhost:8083" -clientes=30 -assentos=10
```

Parâmetros opcionais do simulador:
- `-servers`: lista de endereços TCP dos servidores das empresas (padrão: `localhost:8081,localhost:8082,localhost:8083`)
- `-clientes`: número de passageiros simultâneos (padrão: `30`)
- `-assentos`: número de vagas ofertadas por trecho na semeadura (padrão: `10`)
- `-seed`: `true`/`false` para publicar automaticamente as caronas antes do disparo (padrão: `true`)

---

### Executando os Testes Automatizados

1. **Teste Automatizado do Consórcio (2PC sob Carga + Injeção de Falhas + Recovery)** — não exige servidores externos pré-iniciados (sobe instâncias HTTP em memória e injeta quedas automaticamente):
   ```powershell
   go test ./testes -run TestConsorcio2PCSobCargaEInjecaoDeFalhas -v
   ```
   Verifica que:
   - Nenhum assento é confirmado para mais de um passageiro sob disputa simultânea;
   - Nenhum itinerário é confirmado pela metade quando o servidor de uma empresa falha no meio da transação;
   - Nenhum assento permanece bloqueado após a queda e recuperação de um servidor.

2. **Suíte Completa de Testes (Unitários + Integração TCP + Consórcio)** — com um servidor rodando na porta configurada (ex.: `:8080` ou `:8081`):
   ```powershell
   go test ./testes/... -v -args -server=localhost:8081
   ```

---

## Autor

Trabalho desenvolvido por **Thaylane da Silva** para a disciplina **TEC502 — Concorrência e Conectividade** (UEFS).
docker run --rm -p 8083:8083 -p 9083:9083 vaijunto-servidor:latest -id=empresaC -tcp=:8083 -http=:9083 -url=http://172.16.201.5:9083 -peers=http://172.16.201.10:9081,http://172.16.201.4:9082

docker run --rm -p 8082:8082 -p 9082:9082 vaijunto-servidor:latest -id=empresaB -tcp=:8082 -http=:9082 -url=http://172.16.201.4:9082 -peers=http://172.16.201.10:9081,http://172.16.201.5:9083