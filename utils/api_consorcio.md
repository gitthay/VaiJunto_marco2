# Especificação da API REST do Consórcio — VaiJunto (Problema 2)

Este documento descreve o protocolo de comunicação distribuída baseado em **API REST (HTTP/JSON)** utilizado entre os servidores das empresas do consórcio **VaiJunto**.

Enquanto os clientes (motoristas e passageiros) continuam se comunicando exclusivamente com o servidor de sua própria empresa via **Sockets TCP**, os servidores das empresas cooperam de forma **totalmente descentralizada** (sem coordenador fixo e sem banco de dados compartilhado) por meio desta API REST para:
1. **Monitoramento de saúde (Heartbeat)** e detecção de falhas por timeout;
2. **Descoberta federada de trechos disponíveis** para montagem do grafo de rotas;
3. **Confirmação atômica de itinerários inter-empresas via Two-Phase Commit (2PC)** com resolução determinística de conflitos baseada em **Relógio Lógico de Lamport**;
4. **Recuperação de falhas (Recovery)** e liberação automática de assentos bloqueados (*Presumed Abort*);
5. **Propagação de cancelamentos** de reservas e de caronas entre empresas.

---

## 1. Visão Geral dos Recursos (Endpoints REST)

| Método | Recurso | Descrição | Códigos de Retorno |
|---|---|---|---|
| `GET` | `/api/heartbeat` | Verifica se o servidor parceiro está ativo e sincroniza o relógio lógico de Lamport | `200 OK` |
| `GET` | `/api/caronas` | Retorna os trechos ativos com assentos livres daquela empresa para uma data | `200 OK`, `500 Internal Server Error` |
| `POST` | `/api/2pc/prepare` | **Fase 1 do 2PC (Votação)**: valida disponibilidade, aplica precedência de Lamport e bloqueia provisoriamente os assentos locais (`PREPARED`) | `200 OK`, `400 Bad Request`, `409 Conflict` |
| `POST` | `/api/2pc/commit` | **Fase 2 do 2PC (Confirmação)**: efetiva definitivamente a reserva dos assentos preparados (`COMMITTED`) | `200 OK`, `400 Bad Request`, `409 Conflict` |
| `POST` | `/api/2pc/abort` | **Fase 2 do 2PC (Rollback)**: cancela a pré-reserva e devolve imediatamente os assentos bloqueados (`ABORTED`) | `200 OK`, `400 Bad Request` |
| `GET` | `/api/2pc/status` | Consulta a decisão final de uma transação no servidor coordenador após recuperação de falha | `200 OK` |
| `POST` | `/api/reservas/liberar` | Devolve assentos locais quando um passageiro (ou outro trecho do itinerário) cancela uma reserva confirmada | `200 OK`, `400 Bad Request` |
| `POST` | `/api/caronas/cancelada` | Notifica o servidor de origem do passageiro que uma carona remota foi cancelada pelo motorista | `200 OK`, `400 Bad Request` |

---

## 2. Detalhamento dos Endpoints

### 2.1 `GET /api/heartbeat`
Utilizado pela rotina periódica de *Batimento Cardíaco* (a cada `1.5s`, com timeout de `1.2s`) para verificar se os servidores parceiros estão operacionais e atualizar o Relógio Lógico de Lamport.

* **Resposta (`200 OK`)**:
```json
{
  "status": "ONLINE",
  "empresa_id": "empresaA",
  "url": "http://localhost:9081",
  "lamport": 42
}
```

---

### 2.2 `GET /api/caronas?dia=DD&mes=MM&ano=AAAA`
Retorna apenas as arestas de trechos ativos e com assentos disponíveis (`assentos_livres > 0`) pertencentes àquela empresa na data solicitada, sem expor dados sensíveis de usuários.

* **Parâmetros de Query**: `dia`, `mes`, `ano` (inteiros).
* **Resposta (`200 OK`)**:
```json
[
  {
    "carona_id": "CAR-1793241197280544300",
    "motorista_id": "thay",
    "empresa_id": "empresaA",
    "servidor_url": "http://localhost:9081",
    "origem": "Salvador",
    "destino": "Feira de Santana",
    "horario_partida": "2026-10-15T08:00:00-03:00",
    "horario_chegada": "2026-10-15T09:30:00-03:00",
    "preco": 25.0,
    "assentos_livres": 2
  }
]
```

---

### 2.3 `POST /api/2pc/prepare` (Fase 1 do Two-Phase Commit)
Solicita o bloqueio temporário dos assentos dos trechos sob responsabilidade do servidor participante.

* **Critério de Precedência na Disputa pelo Último Assento**:
  Cada transação recebe no seu início a tupla de prioridade `(timestamp_lamport, empresa_origem_id, transacao_id)`. Caso dois passageiros de empresas distintas disputem simultaneamente o último assento de um trecho durante a fase `PREPARED`, o servidor detentor da carona compara as tuplas: se a nova requisição tiver **maior precedência** (menor valor de `timestamp_lamport`, desempatando lexicograficamente por `empresa_origem_id` e `transacao_id`), ela **preempciona (aborta)** a transação `PREPARED` concorrente e assume o assento.
* **Descentralização de Dados**: O servidor participante recebe e grava em seu `transacoesgerais.json` **apenas as referências aos seus próprios trechos locais** (`trechos_locais`).

* **Requisição**:
```json
{
  "transacao_id": "TX-empresaA-1793241407138189400",
  "reserva_id": "RES-1793241407138189400",
  "passageiro_id": "maria@email.com",
  "empresa_origem_id": "empresaA",
  "coordenador_url": "http://localhost:9081",
  "timestamp_lamport": 105,
  "trechos_locais": [
    {
      "carona_id": "CAR-1793241282878820200",
      "origem": "Feira de Santana",
      "destino": "Vitória da Conquista"
    }
  ]
}
```

* **Resposta — Sucesso / Voto Favorável (`200 OK`)**:
```json
{
  "sucesso": true,
  "estado": "PREPARED",
  "mensagem": "Assentos pré-reservados (PREPARED).",
  "lamport": 106
}
```

* **Resposta — Falha / Sem Assento Disponível (`409 Conflict`)**:
```json
{
  "sucesso": false,
  "estado": "ABORTED",
  "mensagem": "o trecho de Feira de Santana para Vitória da Conquista não possui mais assentos disponíveis",
  "lamport": 106
}
```

---

### 2.4 `POST /api/2pc/commit` (Fase 2 — Confirmação)
Enviado pelo servidor coordenador da compra após **todos** os servidores envolvidos no itinerário responderem `200 OK (PREPARED)`. Muda o estado da transação em `transacoesgerais.json` para `COMMITTED`.

* **Requisição**:
```json
{
  "transacao_id": "TX-empresaA-1793241407138189400",
  "reserva_id": "RES-1793241407138189400",
  "lamport": 108
}
```

* **Resposta (`200 OK`)**:
```json
{
  "sucesso": true,
  "estado": "COMMITTED",
  "mensagem": "Transação confirmada (COMMITTED).",
  "lamport": 109
}
```
*(Caso a transação tenha sofrido preempção ou expirado antes do commit, retorna `409 Conflict` com `"estado": "ABORTED"`).*

---

### 2.5 `POST /api/2pc/abort` (Fase 2 — Rollback)
Enviado pelo servidor coordenador caso **qualquer** servidor participante vote `ABORTED` (409) ou sofra falha/timeout na Fase 1. Devolve imediatamente os assentos locais pré-reservados e marca a transação como `ABORTED`.

* **Requisição**:
```json
{
  "transacao_id": "TX-empresaA-1793241407138189400",
  "reserva_id": "RES-1793241407138189400",
  "lamport": 108
}
```

* **Resposta (`200 OK`)**:
```json
{
  "sucesso": true,
  "estado": "ABORTED",
  "mensagem": "Transação abortada e assentos devolvidos.",
  "lamport": 109
}
```

---

### 2.6 `GET /api/2pc/status?transacao_id=TX-...`
Usado na recuperação de falhas (*Recovery*). Quando um servidor participante possui uma transação em estado `PREPARED` cujo timeout expirou (ou ao reiniciar após uma queda), ele consulta o `coordenador_url` daquela transação para descobrir se a decisão final foi `COMMITTED` ou `ABORTED` (aplicando *Presumed Abort* caso o coordenador não a tenha confirmado).

* **Resposta (`200 OK`)**:
```json
{
  "sucesso": true,
  "estado": "ABORTED",
  "mensagem": "Transação não encontrada no coordenador (Presumed Abort)",
  "lamport": 115
}
```

---

### 2.7 `POST /api/reservas/liberar`
Chamado quando um passageiro cancela sua reserva confirmada (ou quando uma reserva composta é desfeita devido ao cancelamento de outro motorista), instruindo o servidor detentor do trecho a incrementar `AssentosLivre++` e marcar a transação associada como `ABORTED`.

* **Requisição**:
```json
{
  "reserva_id": "RES-1793241407138189400",
  "trechos": [
    {
      "carona_id": "CAR-1793241282878820200",
      "origem": "Feira de Santana",
      "destino": "Vitória da Conquista"
    }
  ]
}
```

* **Resposta (`200 OK`)**:
```json
{
  "sucesso": true
}
```

---

### 2.8 `POST /api/caronas/cancelada`
Chamado quando um motorista cancela sua carona que possuía passageiros de outra empresa. O servidor do motorista devolve os assentos locais e envia esta requisição ao `coordenador_url` (servidor da empresa do passageiro), que remove a reserva do `reservas.json` do passageiro e aciona `/api/reservas/liberar` nos demais servidores que compunham aquele itinerário.

* **Requisição**:
```json
{
  "reserva_id": "RES-1793241407138189400",
  "carona_id": "CAR-1793241197280544300"
}
```

* **Resposta (`200 OK`)**:
```json
{
  "sucesso": true
}
```

---

## 3. Diagrama de Sequência — Confirmação de Itinerário Inter-Empresas (2PC)

Cenário: Um passageiro conectado ao **Servidor Empresa A** deseja buscar e reservar um itinerário de **Salvador a Montes Claros**, composto por:
- **Trecho 1** (`Salvador -> Feira de Santana`): Carona da **Empresa A**
- **Trecho 2** (`Feira de Santana -> Vitória da Conquista`): Carona da **Empresa B**
- **Trecho 3** (`Vitória da Conquista -> Montes Claros`): Carona da **Empresa C**

```mermaid
sequenceDiagram
    autonumber
    actor P as Passageiro (Cliente TCP)
    participant A as Servidor Empresa A<br/>(Coordenador da Transação)
    participant B as Servidor Empresa B<br/>(Participante)
    participant C as Servidor Empresa C<br/>(Participante)

    Note over P,C: 1. Descoberta e Montagem do Grafo Federado (BFS)
    P->>A: TCP: BUSCAR_ITINERARIO (Salvador -> Montes Claros)
    par Consulta Federada via REST
        A->>B: GET /api/caronas?dia=15&mes=10&ano=2026
        B-->>A: 200 OK (Trecho: Feira -> Conquista)
    and
        A->>C: GET /api/caronas?dia=15&mes=10&ano=2026
        C-->>A: 200 OK (Trecho: Conquista -> Montes Claros)
    end
    A->>A: MontarGrafo(Locais + B + C) & BFS
    A-->>P: TCP: MensagemResposta (Itinerário Composto A + B + C)

    Note over P,C: 2. Fase 1 do Two-Phase Commit (PREPARE com Timestamp de Lamport)
    P->>A: TCP: RESERVAR_TRECHO (Itinerário A + B + C)
    A->>A: IncrementarLamport() & Gerar TransacaoID
    A->>A: ExecutarPrepareLocal (Trecho 1: Salvador -> Feira)<br/>Desconta assento e salva PREPARED em transacoesgerais.json

    A->>B: POST /api/2pc/prepare (TransacaoID, Lamport, Trecho 2)
    B->>B: Valida vaga / Precedência Lamport<br/>Desconta assento e salva PREPARED em transacoesgerais.json
    B-->>A: 200 OK {"sucesso": true, "estado": "PREPARED"}

    A->>C: POST /api/2pc/prepare (TransacaoID, Lamport, Trecho 3)
    alt Todos os trechos possuem assento disponível
        C->>C: Valida vaga / Precedência Lamport<br/>Desconta assento e salva PREPARED em transacoesgerais.json
        C-->>A: 200 OK {"sucesso": true, "estado": "PREPARED"}

        Note over A,C: 3A. Fase 2 do Two-Phase Commit — Decisão: COMMIT
        A->>A: ExecutarCommitLocal(TransacaoID) -> COMMITTED
        A->>B: POST /api/2pc/commit (TransacaoID, ReservaID)
        B->>B: Atualiza transacoesgerais.json -> COMMITTED
        B-->>A: 200 OK {"estado": "COMMITTED"}
        A->>C: POST /api/2pc/commit (TransacaoID, ReservaID)
        C->>C: Atualiza transacoesgerais.json -> COMMITTED
        C-->>A: 200 OK {"estado": "COMMITTED"}
        A->>A: Grava Reserva em reservas.json (Empresa A)
        A-->>P: TCP: Sucesso! Reserva confirmada!
    else Falha ou Sem Vaga na Empresa C (ou Timeout)
        C-->>A: 409 Conflict {"sucesso": false, "estado": "ABORTED"}

        Note over A,C: 3B. Fase 2 do Two-Phase Commit — Decisão: ROLLBACK (ABORT)
        A->>A: ExecutarAbortLocal(TransacaoID)<br/>Devolve assento do Trecho 1 e marca ABORTED
        A->>B: POST /api/2pc/abort (TransacaoID)
        B->>B: Devolve assento do Trecho 2 e marca ABORTED
        B-->>A: 200 OK {"estado": "ABORTED"}
        A-->>P: TCP: Erro! Reserva cancelada (nenhum trecho mantido)
    end
```

