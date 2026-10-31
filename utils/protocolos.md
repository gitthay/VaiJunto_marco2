# Protocolo de Aplicação — VaiJunto

Este documento especifica o protocolo de aplicação usado na comunicação entre os clientes (motorista e passageiro) e o servidor de sua empresa no VaiJunto. A comunicação cliente-servidor ocorre sobre **sockets TCP nativos**, sem uso de frameworks de RPC ou mensageria, enquanto a cooperação entre os servidores das empresas do consórcio ocorre via API REST (documentada em [`utils/api_consorcio.md`](api_consorcio.md)). Os dados trafegam como **objetos JSON** enviados diretamente sobre o stream TCP.

## 1. Transporte, Conexão e Fila de Requisições

- O servidor escuta na porta TCP configurada (`:8080`, `:8081`, `:8082` ou `:8083` via `docker-compose.yml` / flags de linha de comando).
- O cliente estabelece uma conexão TCP (`net.Dial("tcp", <host>:<porta>)`) e a mantém **aberta durante toda a sessão do usuário**, enviando uma mensagem por vez e aguardando a resposta antes de enviar a próxima.
- **Fila de Requisições (`filaRequisicoes` + Pool de Workers)**: Cada conexão ativa possui uma goroutine leitora que lê o pacote TCP e enfileira a requisição em um canal bufferizado (`chan TarefaRequisicao`, capacidade de 2048 requisições), processado por um *Pool de 16 Workers* (`main.go`), garantindo estabilidade mesmo sob centenas de clientes simultâneos.
- Não há handshake de aplicação: a primeira mensagem enviada pelo cliente já é uma requisição de `AUTENTICAR`.

### 1.1 Fluxo de conexão

```
Cliente                                   Servidor (Empresa Local)
   |------------- TCP connect ------------->|
   |                                         | goroutine leitora + Fila de Workers
   |------ AUTENTICAR {email, senha} ------->|
   |                                         | valida login simultâneo (bloqueia
   |                                         | se o e-mail já estiver conectado)
   |                                         | autentica ou cadastra o usuário
   |<----------- MensagemResposta -----------|
   |                                         |
   |---- (demais ações do menu) ----------->|
   |<----------- MensagemResposta -----------|
   |                 ...                     |
```

### 1.2 Fluxo de desconexão

- **Encerramento normal**: o cliente fecha a conexão (`conexao.Close()`) ao escolher a opção "Sair" do menu. O servidor detecta `io.EOF` na leitura, libera a sessão do usuário (`RemoverLogin`) e finaliza a goroutine.
- **Queda abrupta**: se a conexão cair sem aviso (crash do cliente, rede interrompida), a leitura no servidor retorna erro; o `defer` da goroutine libera a sessão do usuário da mesma forma, garantindo que o e-mail fique disponível para novo login e que nenhum arquivo fique em estado inconsistente.
- **Mensagem malformada**: se o payload recebido não for um JSON válido, o servidor registra o erro no log e encerra a conexão daquele cliente. Os demais clientes conectados não são afetados.

## 2. Envelope das mensagens

Toda requisição do cliente para o servidor segue o formato:

```json
{
  "acao": "NOME_DA_ACAO",
  "usuario": "email@exemplo.com",
  "payload": { }
}
```

| Campo | Tipo | Descrição |
|---|---|---|
| `acao` | string | Identifica a operação desejada (ver tabela da seção 3) |
| `usuario` | string | E-mail do usuário autenticado que originou a requisição |
| `payload` | objeto (JSON bruto) | Dados específicos da operação; formato varia por `acao` |

Toda resposta do servidor para o cliente segue o formato:

```json
{
  "sucesso": true,
  "mensagem": "Texto descritivo do resultado",
  "payload": { }
}
```

| Campo | Tipo | Descrição |
|---|---|---|
| `sucesso` | bool | Indica se a operação foi concluída com êxito |
| `mensagem` | string | Mensagem legível para exibição ao usuário (omitido se vazio) |
| `payload` | objeto (JSON bruto) | Dados de retorno da operação, quando aplicável (omitido se vazio) |

## 3. Ações do protocolo

| Ação | Quem envia | Requer autenticação prévia |
|---|---|---|
| `AUTENTICAR` | Motorista / Passageiro | Não (é a própria autenticação) |
| `PUBLICAR_CARONA` | Motorista | Sim |
| `CONSULTAR_CARONAS` | Motorista | Sim |
| `CANCELAR_CARONA` | Motorista | Sim |
| `CONSULTAR_PASSAGEIROS_CARONA` | Motorista | Sim |
| `BUSCAR_ITINERARIO` | Passageiro | Sim |
| `RESERVAR_TRECHO` | Passageiro | Sim |
| `LISTAR_RESERVAS` | Passageiro | Sim |
| `CANCELAR_RESERVA` | Passageiro | Sim |

---

### 3.1 `AUTENTICAR`

Autentica um usuário existente ou o cadastra automaticamente no primeiro acesso. Bloqueia se o e-mail já tiver uma sessão ativa em outra conexão.

**Payload da requisição**

| Campo | Tipo | Descrição |
|---|---|---|
| `email` | string | E-mail do usuário |
| `senha` | string | Senha em texto plano (prototipagem acadêmica) |
| `tipo` | string | `"MOTORISTA"` ou `"PASSAGEIRO"` |

**Exemplo — requisição**
```json
{
  "acao": "AUTENTICAR",
  "usuario": "carlos@email.com",
  "payload": {
    "email": "carlos@email.com",
    "senha": "123456",
    "tipo": "MOTORISTA"
  }
}
```

**Exemplo — resposta (sucesso, novo cadastro)**
```json
{
  "sucesso": true,
  "mensagem": "Usuário cadastrado e autenticado com sucesso!"
}
```

**Exemplo — resposta (erro, sessão já ativa)**
```json
{
  "sucesso": false,
  "mensagem": "Acesso negado: Este usuário já está conectado em outra sessão."
}
```

---

### 3.2 `PUBLICAR_CARONA`

O motorista publica uma nova carona com um ou mais trechos no `caronas.json` de sua empresa.

**Payload da requisição** (estrutura `Carona`)

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | string | Gerado pelo cliente (`CAR-<timestamp>`) |
| `motorista_id` | string | E-mail do motorista |
| `assentos_tot` | int | Capacidade total do veículo |
| `ativa` | bool | Sempre `true` na publicação |
| `trechos` | array de `Trecho` | Sequência ordenada de trechos da rota |

Cada `Trecho` contém: `origem`, `destino`, `data_partida` (`{dia, mes, ano}`), `horario_partida`, `horario_chegada` (RFC 3339), `preco`, `assentos_livres`.

**Exemplo — requisição**
```json
{
  "acao": "PUBLICAR_CARONA",
  "usuario": "carlos@email.com",
  "payload": {
    "id": "CAR-1737049200000000000",
    "motorista_id": "carlos@email.com",
    "assentos_tot": 3,
    "ativa": true,
    "trechos": [
      {
        "origem": "Salvador",
        "destino": "Feira de Santana",
        "data_partida": { "dia": 15, "mes": 10, "ano": 2026 },
        "horario_partida": "2026-10-15T08:00:00Z",
        "horario_chegada": "2026-10-15T09:30:00Z",
        "preco": 25.0,
        "assentos_livres": 3
      }
    ]
  }
}
```

**Exemplo — resposta**
```json
{
  "sucesso": true,
  "mensagem": "Carona cadastrada com sucesso!"
}
```

---

### 3.3 `CONSULTAR_CARONAS`

Lista todas as caronas (ativas e canceladas) publicadas pelo motorista autenticado.

**Exemplo — requisição**
```json
{
  "acao": "CONSULTAR_CARONAS",
  "usuario": "carlos@email.com"
}
```

---

### 3.4 `CANCELAR_CARONA`

Cancela uma carona publicada. O servidor verifica reservas locais e consulta via REST (`GET /api/caronas/passageiros`) os servidores parceiros do consórcio. Se houver passageiros com reserva confirmada, devolve um aviso e exige confirmação explícita (`confirmar: true`). Ao confirmar, desativa a carona e notifica os servidores parceiros via `POST /api/caronas/cancelada`.

**Payload da requisição**

| Campo | Tipo | Descrição |
|---|---|---|
| `carona_id` | string | ID da carona a cancelar |
| `confirmar` | bool | `false` na primeira tentativa; `true` para confirmar apesar dos passageiros afetados |

---

### 3.5 `CONSULTAR_PASSAGEIROS_CARONA`

Lista os e-mails dos passageiros (locais e de outras empresas do consórcio via `GET /api/caronas/passageiros`) com reserva confirmada em algum trecho da carona.

**Payload da requisição**

| Campo | Tipo | Descrição |
|---|---|---|
| `carona_id` | string | ID da carona consultada |

---

### 3.6 `BUSCAR_ITINERARIO`

O passageiro busca itinerários entre uma origem e um destino em uma data específica. O servidor consulta suas caronas locais e consulta via REST (`GET /api/caronas`) todas as empresas parceiras online do consórcio, montando um grafo federado e retornando os caminhos encontrados via busca em largura (BFS).

**Payload da requisição**

| Campo | Tipo | Descrição |
|---|---|---|
| `origem` | string | Cidade de origem (comparação sem distinção de maiúsculas/espaços) |
| `destino` | string | Cidade de destino |
| `data` | objeto `Data` | `{ "dia": int, "mes": int, "ano": int }` |

---

### 3.7 `RESERVAR_TRECHO`

Confirma a reserva de um itinerário completo (um ou mais trechos, locais ou inter-empresas) escolhido pelo passageiro. A confirmação é **atômica via Two-Phase Commit (2PC)**: todos os trechos são reservados ou nenhum é.

**Payload da requisição** (estrutura `Itinerario`)

| Campo | Tipo | Descrição |
|---|---|---|
| `trechos` | array de `ArestaTrecho` | Trechos que compõem o itinerário desejado (incluindo `empresa_id` e `servidor_url`) |
| `preco_total` | float | Soma dos preços dos trechos |

---

### 3.8 `LISTAR_RESERVAS`

Lista as reservas confirmadas do passageiro autenticado salvas no `reservas.json` de sua empresa.

---

### 3.9 `CANCELAR_RESERVA`

Cancela uma reserva do passageiro, devolve os assentos locais e aciona `POST /api/reservas/liberar` nos demais servidores do consórcio que possuíam trechos naquele itinerário.

## 4. Log de auditoria

Todas as conexões, requisições, eventos do consórcio (Heartbeat, 2PC, Precedência de Lamport e Recovery) e respostas são registrados no `servidor.log` da respectiva empresa.
