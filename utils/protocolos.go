package utils

import (
	"encoding/json"
	"time"
)

// Data representa estritamente o dia, mês e ano
type Data struct {
	Dia int `json:"dia"`
	Mes int `json:"mes"`
	Ano int `json:"ano"`
}

// trechos individuais de uma carona, que podem ser reservados individualmente
type Trecho struct {
	Origem         string    `json:"origem"`
	Destino        string    `json:"destino"`
	DataPartida    Data      `json:"data_partida"`    // Aponta exclusivamente para o Dia/Mês/Ano
	HorarioPartida time.Time `json:"horario_partida"` // Define a hora/minuto da saída
	HorarioChegada time.Time `json:"horario_chegada"` // Define a hora/minuto da chegada
	Preco          float64   `json:"preco"`
	AssentosLivre  int       `json:"assentos_livres"`
}

// estrutura que guarda a carona
type Carona struct {
	ID          string   `json:"id"`           // Identificador único da carona
	MotoristaID string   `json:"motorista_id"` // ID / Email do motorista
	AssentosTot int      `json:"assentos_tot"` // Capacidade total do veículo
	Trechos     []Trecho `json:"trechos"`      // Sequência de trechos da rota
	Ativa       bool     `json:"ativa"`        // Status se a carona está visível
}

// estrutura de reserva
type Reserva struct {
	ID           string         `json:"id"`
	PassageiroID string         `json:"passageiro_id"`
	Itinerario   []ArestaTrecho `json:"itinerario"`
	ValorTotal   float64        `json:"valor_total"`
	DataCriacao  time.Time      `json:"data_criacao"`
}

// EstadoTransacao representa o estágio do Two-Phase Commit (2PC) em memória
type EstadoTransacao string

const (
	EstadoPrepared  EstadoTransacao = "PREPARED"
	EstadoCommitted EstadoTransacao = "COMMITTED"
	EstadoAborted   EstadoTransacao = "ABORTED"
)

// ReferenciaTrechoLocal identifica um trecho local durante as requisições REST do 2PC
type ReferenciaTrechoLocal struct {
	CaronaID string `json:"carona_id"`
	Origem   string `json:"origem"`
	Destino  string `json:"destino"`
}

// Transacao2PC mantém em memória (RAM) o controle de bloqueio temporário durante o protocolo 2PC
type Transacao2PC struct {
	TransacaoID      string                  `json:"transacao_id"`
	ReservaID        string                  `json:"reserva_id,omitempty"`
	PassageiroID     string                  `json:"passageiro_id"`
	EmpresaOrigemID  string                  `json:"empresa_origem_id"`
	CoordenadorURL   string                  `json:"coordenador_url"`
	TimestampLamport uint64                  `json:"timestamp_lamport"`
	Estado           EstadoTransacao         `json:"estado"`
	TrechosLocais    []ReferenciaTrechoLocal `json:"trechos_locais,omitempty"`
	CriadoEm         time.Time               `json:"criado_em"`
	ExpiraEm         time.Time               `json:"expira_em"`
}

// TipoAcao representa o tipo de ação que o cliente deseja realizar no servidor
type TipoAcao string

const (
	AcaoAutenticar                 TipoAcao = "AUTENTICAR"
	AcaoPublicarCarona             TipoAcao = "PUBLICAR_CARONA"             //feito
	AcaoConsultarCaronas           TipoAcao = "CONSULTAR_CARONAS"           //feito
	AcaoCancelarCarona             TipoAcao = "CANCELAR_CARONA"             //feito
	AcaoBuscarItinerario           TipoAcao = "BUSCAR_ITINERARIO"           //feito
	AcaoReservarTrecho             TipoAcao = "RESERVAR_TRECHO"             //feito
	AcaoListarReservas             TipoAcao = "LISTAR_RESERVAS"             // feito
	AcaoCancelarReserva            TipoAcao = "CANCELAR_RESERVA"            //feito
	AcaoConsultarPassageirosCarona TipoAcao = "CONSULTAR_PASSAGEIROS_CARONA"
)

type TipoUsuario string

const (
	TipoMotorista  TipoUsuario = "MOTORISTA"
	TipoPassageiro TipoUsuario = "PASSAGEIRO"
)

type Usuario struct {
	Email string      `json:"email"`
	Senha string      `json:"senha"`
	Tipo  TipoUsuario `json:"tipo"`
}

// envelope de dados enviado pelo Cliente ao Servidor.
type MensagemRequisicao struct {
	Acao    TipoAcao        `json:"acao"`
	Usuario string          `json:"usuario"`
	Payload json.RawMessage `json:"payload"` //se eu fixar uma struct especifica, teria que crair uma struct para cada tipo de payload, então uso RawMessage
}

// envelope padrão retornado pelo Servidor.
type MensagemResposta struct {
	Sucesso  bool            `json:"sucesso"`
	Mensagem string          `json:"mensagem,omitempty"` //se o campo estiver vazio, não será incluído no JSON
	Payload  json.RawMessage `json:"payload,omitempty"`
}
