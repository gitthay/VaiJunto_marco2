# VaiJunto — Consórcio de Caronas Compartilhadas 🚗💨

Sistema distribuído de caronas compartilhadas para viagens de média e longa distância, desenvolvido para a disciplina **TEC502 — Concorrência e Conectividade** (**Problema 1** e **Problema 2: Consórcio de Caronas Compartilhadas**).

O projeto implementa múltiplos servidores independentes em Go (representando empresas distintas de um consórcio regional), onde:
- **Clientes (motoristas e passageiros)** conectam-se exclusivamente ao servidor de sua própria empresa via **Sockets TCP puros** (JSON sobre TCP), com processamento ordenado por **Fila de Requisições (Worker Pool)** e **detecção de queda com reconexão automática**.
- **Servidores das empresas do consórcio** cooperam de forma **100% descentralizada via requisições REST (HTTP/JSON)** utilizando **Batimento Cardíaco (Heartbeat)**, **Busca de Grafo Federada (BFS)**, **Two-Phase Commit (2PC)** com critério de precedência por **Relógio Lógico de Lamport**, recuperação automática de falhas (*Recovery* / *Presumed Abort*), **reconciliação pós-queda** e consultas/cancelamentos cruzados via REST.

---

## Principais Funcionalidades

- **Descentralização e Isolamento Estrito de Dados**: Cada servidor cria e mantém exclusivamente os seus próprios arquivos (`caronas.json`, `reservas.json`, `usuarios.json` e `servidor.log`) dentro de `dados/<empresa>/` ao ser iniciado, sem banco de dados compartilhado, sem arquivo extra de transações e sem servidor central.
- **Reservas Descentralizadas (cada servidor guarda apenas o que é seu)**:
  - Ao confirmar uma reserva (`2PC COMMIT`), **cada servidor participante** grava a reserva no **seu próprio** `reservas.json`.
  - Os trechos pertencentes àquele servidor são salvos com detalhes completos (origem, destino, motorista, horários, preço).
  - Os trechos pertencentes a **outros servidores** guardam **apenas a referência**: `carona_id`, `empresa_id` e `servidor_url` (sem origem, destino ou demais dados, pois o ID da carona já é único).
  - Exemplo do `reservas.json` da Empresa A para uma reserva A + B:
    ```json
    {
      "id": "RES-1793...",
      "passageiro_id": "maria@email.com",
      "coordenador_url": "http://192.168.1.10:9081",
      "itinerario": [
        {
          "carona_id": "CAR-A-1", "motorista_id": "carlos@email.com", "empresa_id": "empresaA",
          "servidor_url": "http://192.168.1.10:9081", "origem": "Salvador", "destino": "Feira de Santana",
          "horario_partida": "2026-11-25T08:00:00-03:00", "horario_chegada": "2026-11-25T10:00:00-03:00", "preco": 40
        },
        { "carona_id": "CAR-B-7", "empresa_id": "empresaB", "servidor_url": "http://192.168.1.11:9082" }
      ]
    }
    ```
- **Consulta de Detalhes Sob Demanda (Online / Offline)**: Quando o passageiro lista ou vai cancelar suas reservas, o servidor envia apenas `reserva_id` + `carona_id` para o servidor dono do trecho (`GET /api/caronas/detalhe`). Se ele estiver **ONLINE**, os detalhes completos são exibidos; se estiver **OFFLINE**, o cliente mostra `[SERVIDOR OFFLINE - Detalhes indisponíveis no momento]`.
- **Reconciliação Pós-Queda (restauração de `+1` assento)**: Se um servidor estava desligado enquanto uma reserva da qual ele participava foi cancelada (ou teve a carona cancelada) em outro servidor, ao voltar ele consulta `GET /api/reservas/status` nos demais participantes (no boot, a cada ciclo do Heartbeat e antes das consultas), remove a reserva do seu `reservas.json` e devolve `+1` assento no seu `caronas.json` — de forma idempotente (nunca soma duas vezes).
- **Detecção de Queda do Servidor no Cliente + Reconexão Automática**: Se o servidor ao qual o cliente está conectado cair, o cliente **não exibe mais listas vazias**. Ele detecta o fim da conexão TCP (`EOF`/reset) e avisa `[SERVIDOR OFFLINE]`. Quando o servidor volta, na próxima ação do menu o cliente **reconecta, refaz o login automaticamente** e executa a operação pedida (`[RECONECTADO]`), sem precisar reiniciar o terminal.
- **Fila de Requisições com Pool de Workers (`main.go`)**: As requisições TCP dos clientes entram em uma fila concorrente (`chan TarefaRequisicao`) consumida por um pool fixo de *workers*, evitando saturação de I/O sob rajadas de centenas de clientes simultâneos.
- **Batimento Cardíaco (Heartbeat)**: Monitoramento contínuo entre os servidores (`GET /api/heartbeat`). Se uma empresa cair, as demais detectam via timeout e continuam operando para as rotas que não dependem dela.
- **Busca Federada de Itinerários (BFS em Grafo)**: O servidor do passageiro consulta via REST (`GET /api/caronas`) os trechos das empresas parceiras online e monta o grafo unificado para encontrar roteiros combinando múltiplas empresas.
- **Reserva Atômica Distribuída (Two-Phase Commit — 2PC)**:
  - **Fase 1 (`POST /api/2pc/prepare`)**: O servidor do passageiro atua como coordenador e solicita o bloqueio temporário dos assentos em cada empresa envolvida (em ordem determinística de URL para prevenir deadlocks).
  - **Fase 2 (`POST /api/2pc/commit` ou `/api/2pc/abort`)**: Se todas confirmarem, a reserva é efetivada e gravada de forma descentralizada no `reservas.json` de cada participante. Se qualquer uma recusar ou falhar, todas desfazem a pré-reserva (`ABORTED`).
  - O estado `PREPARED` fica apenas em memória (RAM) durante os milissegundos da votação; os dados definitivos ficam em disco em cada máquina. Por isso funciona normalmente com servidores em **PCs diferentes** (inclusive Windows + Linux), já que toda coordenação ocorre pela rede.
- **Critério de Precedência Distribuída (Relógio Lógico de Lamport)**: Cada transação carrega a tupla `(TimestampLamport, EmpresaOrigemID, TransacaoID)`. Em disputas simultâneas pelo último assento, vence de forma determinística o menor timestamp lógico (desempatando por `EmpresaOrigemID` e `TransacaoID`).
- **Tolerância a Falhas e Recuperação via REST**: Caso o coordenador falhe após a fase `PREPARED`, a rotina de *Recovery* consulta `GET /api/2pc/status` ou aplica *Presumed Abort* após o timeout, garantindo que **nenhum assento fique bloqueado indefinidamente**.
- **Consultas e Cancelamentos Entre Empresas 100% via REST**:
  - **Consulta de Passageiros (`GET /api/caronas/passageiros`)**.
  - **Cancelamento de Reserva (`POST /api/reservas/liberar`)**: remove a reserva e devolve `+1` assento em cada participante.
  - **Cancelamento de Carona (`POST /api/caronas/cancelada`)**: desativa a carona e notifica os parceiros, que apagam as reservas afetadas e liberam os assentos dos demais trechos (com reenvio automático quando um parceiro volta a ficar online).

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
- **Protocolo TCP (Cliente ↔ Servidor Local)**: [`utils/protocolos.md`](utils/protocolos.md).
- **API REST entre Servidores + Diagrama de Sequência 2PC**: [`utils/api_consorcio.md`](utils/api_consorcio.md).

---

## Estrutura de Arquivos

```text
redes_marco2/
├── main.go                     # Servidor (Fila de Requisições TCP + Inicialização REST Consórcio)
├── utils/
│   ├── protocolos.go           # Entidades de domínio, envelopes TCP e estrutura em memória do 2PC
│   ├── servicos.go             # Grafo/BFS, persistência isolada, armazenamento descentralizado de reservas
│   ├── consorcio.go            # API REST, Heartbeat, Lamport, 2PC, Recovery, detalhes sob demanda e reconciliação
│   ├── protocolos.md           # Especificação do protocolo TCP cliente-servidor
│   └── api_consorcio.md        # Especificação completa da API REST do consórcio
├── motorista/
│   └── mainmotorista.go        # Cliente CLI do motorista (TCP + detecção de queda e reconexão)
├── passageiro/
│   └── mainpassageiro.go       # Cliente CLI do passageiro (TCP + detecção de queda e reconexão)
├── simulador/
│   └── mainsimulador.go        # Simulador autônomo de passageiros sob carga
├── testes/
│   └── concorrencia_test.go    # Suíte de 8 testes automatizados do Consórcio
├── Dockerfile
├── docker-compose.yml          # Orquestração das instâncias isoladas do consórcio
└── README.md
```

---

## Como Executar

### Pré-requisitos
- **Go 1.22+**
- **Docker e Docker Compose** (opcional)

---

### Opção 1: Subindo com Docker Compose

#### A) Rodando em Computadores Distintos (1 Contêiner por Máquina)
Substitua os IPs pelos IPs reais de cada computador na rede local. Os sistemas operacionais podem ser diferentes (ex.: um PC Windows e outro Linux), pois a comunicação é toda via TCP/HTTP.

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
*(Cada computador cria apenas a pasta `dados/` do servidor iniciado nele).*

> **Firewall do Windows**: se outro PC não conseguir se conectar ao servidor no Windows (`i/o timeout`), libere as portas no PowerShell como Administrador:
> ```powershell
> New-NetFirewallRule -DisplayName "VaiJunto" -Direction Inbound -LocalPort 8081,9081 -Protocol TCP -Action Allow
> ```

#### B) Rodando os 3 Servidores no Mesmo Computador
```bash
docker compose up --build
```
Portas expostas:
- **Empresa A**: TCP `localhost:8081` | HTTP REST `http://localhost:9081`
- **Empresa B**: TCP `localhost:8082` | HTTP REST `http://localhost:9082`
- **Empresa C**: TCP `localhost:8083` | HTTP REST `http://localhost:9083`

---

### Opção 2: Subindo as Instâncias Sem Docker

**No mesmo computador** (3 terminais):
```powershell
go run main.go -id=empresaA -tcp=:8081 -http=:9081 -peers=http://localhost:9082,http://localhost:9083
go run main.go -id=empresaB -tcp=:8082 -http=:9082 -peers=http://localhost:9081,http://localhost:9083
go run main.go -id=empresaC -tcp=:8083 -http=:9083 -peers=http://localhost:9081,http://localhost:9082
```

**Em computadores distintos** (informe a própria URL com `-url` e os IPs dos parceiros em `-peers`):
```bash
# PC 1 (Empresa A)
go run main.go -id=empresaA -tcp=:8081 -http=:9081 -url=http://192.168.1.10:9081 -peers=http://192.168.1.11:9082,http://192.168.1.12:9083

# PC 2 (Empresa B)
go run main.go -id=empresaB -tcp=:8082 -http=:9082 -url=http://192.168.1.11:9082 -peers=http://192.168.1.10:9081,http://192.168.1.12:9083

# PC 3 (Empresa C)
go run main.go -id=empresaC -tcp=:8083 -http=:9083 -url=http://192.168.1.12:9083 -peers=http://192.168.1.10:9081,http://192.168.1.11:9082
```

---

### Conectando os Clientes (Motorista e Passageiro)

```powershell
# Motorista conectando na Empresa A
go run motorista/mainmotorista.go -server 192.168.1.10:8081

# Passageiro conectando na Empresa C
go run passageiro/mainpassageiro.go -server 192.168.1.12:8083
```
*(Na mesma máquina, use `localhost:8081`, `localhost:8082` e `localhost:8083`).*

**Comportamento quando o servidor cai com o cliente aberto:**
```text
[ALERTA DE REDE]: A conexão com o servidor (192.168.1.10:8081) foi perdida! Tentando reconectar...
[SERVIDOR OFFLINE]: O servidor (192.168.1.10:8081) caiu e continua fora do ar no momento. Aguarde ele voltar e tente novamente.
```
Depois que o servidor volta, basta escolher qualquer opção do menu:
```text
[ALERTA DE REDE]: A conexão com o servidor (192.168.1.10:8081) foi perdida! Tentando reconectar...
[RECONECTADO]: Conexão restabelecida automaticamente com o servidor (192.168.1.10:8081)!
```

**Exibição de reserva com um servidor participante offline:**
```text
[1] ID Reserva: RES-1793... | Rota: Salvador -> [Servidor Offline]
    Partida: 25/11/2026 às 08:00 | Valor Total: R$ 150.00 | Trechos: 2
      Trecho 1 [empresaA]: Salvador -> Feira de Santana (Carona ID: CAR-A-1 | Motorista: carlos@email.com | Partida: 08:00 | Chegada: 10:00 | R$ 40.00)
      Trecho 2 [empresaB]: (Carona ID: CAR-B-7 | Servidor: http://192.168.1.11:9082) -> [SERVIDOR OFFLINE - Detalhes indisponíveis no momento]
```

---

### Executando o Simulador Autônomo de Passageiros sob Carga

```powershell
# Em 3 PCs diferentes:
go run simulador/mainsimulador.go -servers 192.168.1.10:8081,192.168.1.11:8082,192.168.1.12:8083 -clientes 300 -assentos 10

# No mesmo PC:
go run simulador/mainsimulador.go -servers localhost:8081,localhost:8082,localhost:8083 -clientes 300 -assentos 10
```

Parâmetros opcionais:
- `-servers`: endereços TCP dos servidores (padrão: `localhost:8081,localhost:8082,localhost:8083`)
- `-clientes`: número de passageiros simultâneos (padrão: `300`)
- `-assentos`: vagas ofertadas por trecho na semeadura (padrão: `10`)
- `-fila`: tamanho máximo da fila de conexões simultâneas (padrão: `45`)
- `-seed`: `true`/`false` para publicar automaticamente as caronas antes do disparo (padrão: `true`)

---

### Executando os Testes Automatizados

A suíte em [`testes/concorrencia_test.go`](testes/concorrencia_test.go) é autossuficiente (sobe servidores REST em pastas temporárias isoladas e injeta falhas):

```powershell
go test ./testes -v
```

Os 8 testes validam:
1. **`Test1_HeartbeatESincronizacaoLamport`**: Monitoramento `ONLINE` via `/api/heartbeat` e avanço do Relógio de Lamport.
2. **`Test2_BuscaFederadaGrafoConsorcio`**: Grafo federado entre 3 empresas e continuidade da busca quando uma fica offline.
3. **`Test3_Concorrencia2PCDisputaUltimoAssentoInterEmpresas`**: 25 passageiros simultâneos disputando 1 vaga, com apenas 1 confirmação e rollback atômico das outras 24.
4. **`Test4_InjecaoDeFalhaParticipanteNoMeioDaReserva`**: Queda de participante no meio do 2PC sem confirmar itinerário pela metade.
5. **`Test5_QuedaCoordenadorERecuperacaoAutomaticaViaREST`**: Recovery via `GET /api/2pc/status` liberando assento preso em `PREPARED`.
6. **`Test6_PrecedenciaRelogioLogicoLamportEDesempate`**: Resolução determinística de conflito por Lamport e desempate por `EmpresaOrigemID`.
7. **`Test7_ConsultaPassageirosECancelamentoEntreEmpresasViaREST`**: Consulta de passageiros e cancelamento de carona em cascata 100% via REST.
8. **`Test8_ArmazenamentoDescentralizadoEReconciliacaoPosQueda`**: Reserva salva de forma descentralizada (trecho remoto apenas com `carona_id`/`empresa_id`/`servidor_url`), detalhes sob demanda com parceiro online/offline e restauração de `+1` assento após reserva cancelada durante a queda.

---

## Autor

Trabalho desenvolvido por **Thaylane da Silva** para a disciplina **TEC502 — Concorrência e Conectividade** (UEFS).
