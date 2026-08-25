package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// ============================================================================
// SFU de áudio e vídeo (estilo Discord): o servidor recebe o microfone, a
// câmera e a tela de cada pessoa e reencaminha para todas as outras da sala.
// Ninguém se conecta direto com ninguém.
//
// A sinalização é toda "orientada a estado": em vez de mandar eventos avulsos
// ("fulano ligou a câmera", "essa track é uma tela"), o servidor manda o
// retrato completo da sala (evento "state") sempre que algo muda. Quem acabou
// de entrar recebe exatamente a mesma coisa que quem já estava lá, então não
// existe "cheguei atrasado e perdi o aviso" — que era a causa de participantes
// não enxergarem a câmera/tela de quem já estava na sala.
// ============================================================================

// Tipos de mídia que uma pessoa pode publicar. Cada um tem seu próprio canal
// (transceiver) reservado desde o início da conexão.
const (
	kindMic         = "mic"
	kindScreen      = "screen"
	kindScreenAudio = "screen-audio"
	kindCam         = "cam"
)

// protocolVersion muda sempre que o formato das mensagens de sinalização muda.
// O servidor manda essa versão no "welcome" e o navegador compara com a dele:
// se não bater, ele recarrega a página sozinho para buscar o cliente novo.
//
// Isso existe porque já aconteceu: o servidor foi atualizado, o navegador
// continuou com o index.html antigo em cache, e a sala ficou meio funcionando
// (o áudio passava, mas a lista de participantes e as câmeras não apareciam)
// sem nenhum erro visível. Ver também noCache, que evita o cache velho.
const protocolVersion = "3"

var (
	upgrader = websocket.Upgrader{CheckOrigin: checkOrigin}

	// roomLock protege peers, tracks, pendingRenegotiation e screenSharer.
	// É um lock só (em vez de um por coleção) porque todas essas coisas mudam
	// juntas: quem entra/sai muda as tracks, que mudam a renegociação.
	roomLock sync.RWMutex

	peers  []*peerState              // participantes, na ordem em que entraram
	tracks = map[string]*roomTrack{} // tudo que está sendo publicado, por ID de track

	// Participantes que ficaram devendo uma oferta porque a anterior ainda não
	// tinha sido respondida quando algo mudou (ver syncLocked e "case answer").
	pendingRenegotiation = map[*peerState]bool{}

	screenSharer *peerState // quem está com a tela compartilhada (nil = ninguém)

	peerSeq atomic.Uint64
)

// peerState é um participante: sua conexão WebRTC, seu websocket e o que ele
// está transmitindo no momento.
type peerState struct {
	id string
	pc *webrtc.PeerConnection
	ws *threadSafeWriter

	// Canais reservados desta pessoa (microfone, áudio da tela, tela e câmera).
	// Cada um nasce com uma track "placeholder" interna do Pion, que não tem
	// dono em `tracks` — então a limpeza genérica de syncLocked os removeria
	// como se fossem lixo, liberando a m-line pro Pion reaproveitar pra
	// encaminhar a mídia de OUTRA pessoa. Quando isso acontece o navegador não
	// consegue mais casar a mídia com o dono (embaralha tela com câmera, ou
	// deixa um microfone mudo), então eles são pulados explicitamente.
	//
	// A chave é a transceiver, não o sender: ao reaproveitar uma m-line o Pion
	// cria um sender NOVO, então comparar senders não detectaria nada.
	ownTransceivers map[*webrtc.RTPTransceiver]bool

	// Estado declarado pelo próprio navegador. O servidor só repassa: é isso
	// que diz aos outros se a câmera está realmente ligada ou se a track
	// continua publicada mas parada.
	muted    bool
	cameraOn bool
}

// roomTrack é uma mídia publicada por alguém, pronta pra ser reencaminhada.
type roomTrack struct {
	local  *webrtc.TrackLocalStaticRTP
	peerID string
	kind   string
}

// threadSafeWriter serializa as escritas no websocket (várias goroutines
// escrevem nele: sinalização, mudanças de estado, candidatos ICE...).
type threadSafeWriter struct {
	*websocket.Conn
	sync.Mutex
}

func (t *threadSafeWriter) WriteJSON(v interface{}) error {
	t.Lock()
	defer t.Unlock()
	return t.Conn.WriteJSON(v)
}

// websocketMessage é o formato das mensagens de sinalização trocadas com o
// navegador. Data carrega JSON serializado quando o evento tem payload.
type websocketMessage struct {
	Event string `json:"event"`
	Data  string `json:"data"`
}

// ---------------------------------------------------------------------------
// Retrato da sala mandado para os navegadores
// ---------------------------------------------------------------------------

type trackInfo struct {
	ID       string `json:"id"`
	StreamID string `json:"streamId"`
	PeerID   string `json:"peerId"`
	Kind     string `json:"kind"`

	// Mid é a m-line em que ESTE destinatário recebe essa mídia — por isso o
	// retrato é montado por pessoa, e não uma vez só para a sala toda.
	//
	// O navegador precisa do mid porque o id da track não é confiável do lado
	// de quem recebe: o Chrome fixa receiver.track.id quando cria a
	// transceiver e não atualiza se a m-line for reaproveitada para outra
	// mídia depois. O mid, esse sim, sempre aponta para o lugar certo.
	// Fica vazio enquanto a m-line ainda não foi negociada; nesse caso o
	// navegador cai para a busca pelo id.
	Mid string `json:"mid,omitempty"`
}

type peerInfo struct {
	PeerID   string `json:"peerId"`
	Muted    bool   `json:"muted"`
	CameraOn bool   `json:"cameraOn"`
}

type roomState struct {
	Peers        []peerInfo  `json:"peers"`
	Tracks       []trackInfo `json:"tracks"`
	ScreenSharer string      `json:"screenSharer"`
}

// welcomeInfo é a primeira mensagem que o navegador recebe: quem ele é nesta
// sala e qual versão do protocolo o servidor fala.
type welcomeInfo struct {
	PeerID   string `json:"peerId"`
	Protocol string `json:"protocol"`
}

func main() {
	http.Handle("/", noCache(http.FileServer(http.Dir("./web"))))
	http.HandleFunc("/ws", websocketHandler)
	http.HandleFunc("/config", configHandler) // entrega STUN/TURN para o navegador

	// O Render (e outros hosts) definem a porta via variável de ambiente PORT.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Sala de voz rodando na porta %s", port)

	server := &http.Server{
		Addr:              ":" + port,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

// noCache obriga o navegador a revalidar os arquivos de web/ a cada carga.
//
// O http.FileServer sozinho manda só Last-Modified, sem Cache-Control — e com
// isso o navegador aplica cache heurístico e pode servir um index.html antigo
// direto do cache, sem nem perguntar ao servidor. Foi exatamente o que
// aconteceu depois de uma atualização do protocolo: servidor novo conversando
// com cliente velho, sala meio quebrada e nenhum erro à vista.
//
// "no-cache" não proíbe guardar o arquivo, só exige revalidar antes de usar —
// então na prática continua sendo um 304 barato quando nada mudou.
func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
		h.ServeHTTP(w, r)
	})
}

// checkOrigin só aceita websockets vindos da própria página (ou de origens
// listadas em ALLOWED_ORIGINS, separadas por vírgula). Antes qualquer site
// podia abrir uma conexão com este servidor.
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // cliente não-navegador (curl, teste automatizado)
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, allowed := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		if allowed = strings.TrimSpace(allowed); allowed != "" && strings.EqualFold(allowed, origin) {
			return true
		}
	}
	log.Printf("websocket recusado: origem %q não confere com o host %q", origin, r.Host)
	return false
}

// iceServers monta a lista de servidores STUN/TURN a partir de variáveis de
// ambiente. STUN sempre entra; o TURN (relay) é opcional, mas é o que faz a
// mídia funcionar em hosts que só expõem HTTP, como o Render.
//
//	TURN_URL  = turn:host:porta  (pode ter vários separados por vírgula)
//	TURN_USER = usuário do TURN
//	TURN_PASS = senha do TURN
func iceServers() []webrtc.ICEServer {
	servers := []webrtc.ICEServer{
		{URLs: []string{"stun:stun.l.google.com:19302"}},
	}
	if turnURL := os.Getenv("TURN_URL"); turnURL != "" {
		servers = append(servers, webrtc.ICEServer{
			URLs:       strings.Split(turnURL, ","),
			Username:   os.Getenv("TURN_USER"),
			Credential: os.Getenv("TURN_PASS"),
		})
	}
	return servers
}

// configHandler entrega ao navegador a mesma lista de STUN/TURN, para que as
// credenciais fiquem só no servidor (variáveis de ambiente) e não no HTML.
func configHandler(w http.ResponseWriter, r *http.Request) {
	type iceServer struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username,omitempty"`
		Credential string   `json:"credential,omitempty"`
	}
	out := struct {
		ICEServers []iceServer `json:"iceServers"`
	}{
		ICEServers: []iceServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	if turnURL := os.Getenv("TURN_URL"); turnURL != "" {
		out.ICEServers = append(out.ICEServers, iceServer{
			URLs:       strings.Split(turnURL, ","),
			Username:   os.Getenv("TURN_USER"),
			Credential: os.Getenv("TURN_PASS"),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ---------------------------------------------------------------------------
// Estado da sala
// ---------------------------------------------------------------------------

// publishTrack registra a mídia recebida de alguém para poder redistribuí-la.
// O ID usado (t.ID(), sem alterar) é o mesmo que aparece no msid do SDP que o
// navegador de quem recebe vai enxergar — é assim que o cliente correlaciona
// "essa track que chegou" com "esse item do retrato da sala".
func publishTrack(p *peerState, t *webrtc.TrackRemote, kind string) (*webrtc.TrackLocalStaticRTP, error) {
	local, err := webrtc.NewTrackLocalStaticRTP(t.Codec().RTPCodecCapability, t.ID(), t.StreamID())
	if err != nil {
		return nil, err
	}

	roomLock.Lock()
	tracks[t.ID()] = &roomTrack{local: local, peerID: p.id, kind: kind}
	roomLock.Unlock()

	signalPeers()
	return local, nil
}

// unpublishTrack tira do ar a mídia de alguém que parou de transmitir ou saiu.
func unpublishTrack(local *webrtc.TrackLocalStaticRTP) {
	roomLock.Lock()
	delete(tracks, local.ID())
	roomLock.Unlock()

	signalPeers()
}

// joinRoom coloca o participante na sala e já responde a ele quem ele é.
// O "welcome" é escrito com o lock tomado justamente pra ele chegar antes de
// qualquer "state": assim o navegador sempre sabe qual dos participantes do
// retrato é ele mesmo.
func joinRoom(p *peerState) error {
	roomLock.Lock()
	defer roomLock.Unlock()

	peers = append(peers, p)

	data, err := json.Marshal(welcomeInfo{PeerID: p.id, Protocol: protocolVersion})
	if err != nil {
		return err
	}
	return p.ws.WriteJSON(&websocketMessage{Event: "welcome", Data: string(data)})
}

// leaveRoom limpa tudo que era desta pessoa: a conexão, as tracks publicadas e
// a reserva do compartilhamento de tela. Fazer isso aqui (e não esperando o
// ciclo de vida das tracks WebRTC) é o que garante que a câmera/tela de quem
// saiu suma da tela dos outros na hora.
func leaveRoom(p *peerState) {
	// A conexão só é fechada DEPOIS de tirar a pessoa da sala: se fechasse
	// antes, uma renegociação em andamento ainda enxergaria esse participante
	// na lista e ficaria tentando (e falhando) mexer numa conexão morta.
	roomLock.Lock()
	for i := range peers {
		if peers[i] == p {
			peers = append(peers[:i], peers[i+1:]...)
			break
		}
	}
	delete(pendingRenegotiation, p)
	for id, t := range tracks {
		if t.peerID == p.id {
			delete(tracks, id)
		}
	}
	if screenSharer == p {
		screenSharer = nil
	}
	roomLock.Unlock()

	_ = p.pc.Close()
	signalPeers()
}

// releaseScreen devolve o "direito" de compartilhar tela para a sala, se for
// essa a pessoa que estava compartilhando.
func releaseScreen(p *peerState) {
	roomLock.Lock()
	if screenSharer != p {
		roomLock.Unlock()
		return
	}
	screenSharer = nil
	roomLock.Unlock()

	broadcastState()
}

// broadcastState manda o retrato da sala para todo mundo.
func broadcastState() {
	roomLock.RLock()
	defer roomLock.RUnlock()
	broadcastStateLocked()
}

// broadcastStateLocked exige roomLock já tomado (leitura ou escrita).
//
// O retrato é montado uma vez por pessoa porque o mid de cada mídia depende de
// quem está recebendo: a mesma câmera chega numa m-line diferente pra cada
// destinatário.
func broadcastStateLocked() {
	for _, p := range peers {
		data, err := json.Marshal(stateForLocked(p))
		if err != nil {
			log.Printf("serializando estado da sala: %v", err)
			return
		}
		if err := p.ws.WriteJSON(&websocketMessage{Event: "state", Data: string(data)}); err != nil {
			log.Printf("enviando estado para %s: %v", p.id, err)
		}
	}
}

// stateForLocked monta o retrato da sala do ponto de vista de dest.
func stateForLocked(dest *peerState) roomState {
	state := roomState{
		Peers:  make([]peerInfo, 0, len(peers)),
		Tracks: make([]trackInfo, 0, len(tracks)),
	}
	for _, p := range peers {
		state.Peers = append(state.Peers, peerInfo{PeerID: p.id, Muted: p.muted, CameraOn: p.cameraOn})
	}

	mids := midsFor(dest)
	for id, t := range tracks {
		state.Tracks = append(state.Tracks, trackInfo{
			ID:       id,
			StreamID: t.local.StreamID(),
			PeerID:   t.peerID,
			Kind:     t.kind,
			Mid:      mids[id],
		})
	}
	if screenSharer != nil {
		state.ScreenSharer = screenSharer.id
	}
	return state
}

// midsFor diz, para cada mídia que este participante recebe, em qual m-line
// ela chega. Um mid vazio (ainda não negociado) simplesmente não entra.
func midsFor(p *peerState) map[string]string {
	out := map[string]string{}
	for _, transceiver := range p.pc.GetTransceivers() {
		if p.ownTransceivers[transceiver] {
			continue // canal reservado: não carrega mídia de outra pessoa
		}
		sender := transceiver.Sender()
		if sender == nil || sender.Track() == nil {
			continue
		}
		if mid := transceiver.Mid(); mid != "" {
			out[sender.Track().ID()] = mid
		}
	}
	return out
}

// signalPeers renegocia com quem precisa e depois publica o retrato da sala.
func signalPeers() {
	roomLock.Lock()
	defer roomLock.Unlock()

	syncLocked()
	broadcastStateLocked()
}

// dropPeerLocked tira da sala o participante na posição i e encerra a conexão
// dele. Exige roomLock tomado para escrita.
func dropPeerLocked(i int) {
	p := peers[i]
	delete(pendingRenegotiation, p)
	peers = append(peers[:i], peers[i+1:]...)
	for id, t := range tracks {
		if t.peerID == p.id {
			delete(tracks, id)
		}
	}
	if screenSharer == p {
		screenSharer = nil
	}
	go func() { _ = p.pc.Close() }()
}

// syncLocked garante que cada participante esteja recebendo a mídia de todos os
// outros, renegociando as conexões quando algo muda. Exige roomLock tomado
// para escrita.
func syncLocked() {
	attempt := func() (tryAgain bool) {
		for i := range peers {
			p := peers[i]

			// Limpa conexões que já fecharam ou desistiram.
			if state := p.pc.ConnectionState(); state == webrtc.PeerConnectionStateClosed ||
				state == webrtc.PeerConnectionStateFailed {
				dropPeerLocked(i)
				return true
			}

			// Só vale a pena renegociar (mandar uma oferta nova) se algo
			// realmente mudou pra essa pessoa, se é a primeira vez (ainda não
			// tem nem uma oferta local), ou se ficamos devendo uma oferta de uma
			// rodada anterior. Sem isso, toda resposta de qualquer participante
			// reiniciava uma rodada de ofertas pra TODOS os outros mesmo sem
			// nada de novo — uma cascata que, com tela e câmera mudando quase ao
			// mesmo tempo, corrompia a negociação.
			changed := p.pc.CurrentLocalDescription() == nil || pendingRenegotiation[p]

			// O que este participante já está recebendo hoje.
			existing := map[string]bool{}
			for _, transceiver := range p.pc.GetTransceivers() {
				if p.ownTransceivers[transceiver] {
					continue
				}
				sender := transceiver.Sender()
				if sender == nil || sender.Track() == nil {
					continue
				}
				existing[sender.Track().ID()] = true

				// Se o dono dessa mídia já saiu (ou parou de publicar), tira daqui.
				if _, ok := tracks[sender.Track().ID()]; !ok {
					if err := p.pc.RemoveTrack(sender); err != nil {
						log.Printf("RemoveTrack (%s): %v — tirando da sala", p.id, err)
						dropPeerLocked(i)
						return true
					}
					changed = true
				}
			}

			// Adiciona o que ainda falta — menos a mídia da própria pessoa, que
			// nunca volta pra ela (senão ela se ouviria/se veria em eco).
			for id, t := range tracks {
				if t.peerID == p.id || existing[id] {
					continue
				}
				if _, err := p.pc.AddTrack(t.local); err != nil {
					// Insistir não resolve: quase sempre é uma conexão que
					// acabou de cair. Se ficássemos tentando, essa pessoa
					// gastaria as 25 tentativas da rodada e atrasaria a
					// renegociação de todo mundo.
					log.Printf("AddTrack (%s): %v — tirando da sala", p.id, err)
					dropPeerLocked(i)
					return true
				}
				changed = true
			}

			if !changed {
				continue
			}

			// Se uma oferta anterior pra essa pessoa ainda não foi respondida,
			// não manda outra agora: chamar CreateOffer/SetLocalDescription de
			// novo nesse estado corrompe a negociação (a resposta antiga não bate
			// mais com a oferta nova). Marcamos como pendente pra tentar de novo
			// assim que a resposta chegar (ver "case answer").
			if p.pc.SignalingState() != webrtc.SignalingStateStable {
				pendingRenegotiation[p] = true
				continue
			}

			offer, err := p.pc.CreateOffer(nil)
			if err != nil {
				log.Printf("CreateOffer (%s): %v", p.id, err)
				pendingRenegotiation[p] = true
				continue
			}
			if err = p.pc.SetLocalDescription(offer); err != nil {
				log.Printf("SetLocalDescription (%s): %v", p.id, err)
				pendingRenegotiation[p] = true
				continue
			}
			offerJSON, err := json.Marshal(offer)
			if err != nil {
				log.Printf("serializando oferta (%s): %v", p.id, err)
				pendingRenegotiation[p] = true
				continue
			}
			if err = p.ws.WriteJSON(&websocketMessage{Event: "offer", Data: string(offerJSON)}); err != nil {
				// Websocket morto: insistir agora não adianta, e a conexão vai
				// ser limpa pelo handler dessa pessoa.
				log.Printf("enviando oferta (%s): %v", p.id, err)
				pendingRenegotiation[p] = true
				continue
			}
			delete(pendingRenegotiation, p)
		}
		return false
	}

	// Tenta sincronizar; se algo mudou no meio, refaz a passada. Depois de
	// muitas tentativas seguidas, desiste por ora e reagenda — reagendar na
	// hora (go signalPeers()) viraria um laço quente consumindo CPU à toa.
	for syncAttempt := 0; ; syncAttempt++ {
		if syncAttempt == 25 {
			time.AfterFunc(3*time.Second, signalPeers)
			return
		}
		if !attempt() {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Conexão de um participante
// ---------------------------------------------------------------------------

// websocketHandler cuida de um participante do início ao fim da conexão.
func websocketHandler(w http.ResponseWriter, r *http.Request) {
	unsafeConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print(err)
		return
	}
	c := &threadSafeWriter{Conn: unsafeConn}
	defer c.Close()

	peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers()})
	if err != nil {
		log.Print(err)
		return
	}
	defer peerConnection.Close()

	// Cada tipo de mídia tem seu próprio canal, reservado desde o primeiro
	// offer/answer: o navegador já negocia todos eles como aptos a enviar, então
	// ligar a câmera ou compartilhar a tela vira só um replaceTrack(), sem nova
	// renegociação — e todos podem ficar ativos ao mesmo tempo.
	//
	// A ORDEM importa: o navegador casa as m-lines desta oferta com as
	// transceivers que ele criou (áudio, áudio, vídeo, vídeo, na mesma ordem),
	// e é por essa correspondência que o servidor sabe se um vídeo que chegou é
	// tela ou câmera.
	//
	// Todas as quatro são "sendrecv", inclusive a do microfone — que só precisa
	// receber. O motivo é sutil: uma transceiver "recvonly" nasce sem sender, e
	// o Pion reaproveita justamente as transceivers sem sender quando precisa
	// de uma m-line pra encaminhar a mídia de outra pessoa. O microfone de um
	// participante acabava grudado na m-line do microfone de quem recebe — e o
	// Chrome fixa o receiver.track.id na criação da transceiver, sem atualizar
	// quando a m-line é reciclada. Resultado: o id anunciado pelo servidor
	// nunca batia com o do navegador e o áudio daquela pessoa não tocava.
	// Nascer com uma track placeholder (o que "sendrecv" faz) tira a transceiver
	// da fila de reaproveitamento. Ver também ownSenders e trackInfo.Mid.
	micTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}
	screenAudioTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}
	screenTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}
	cameraTransceiver, err := peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		log.Print(err)
		return
	}

	p := &peerState{
		id: nextPeerID(),
		pc: peerConnection,
		ws: c,
		ownTransceivers: map[*webrtc.RTPTransceiver]bool{
			micTransceiver:         true,
			screenAudioTransceiver: true,
			screenTransceiver:      true,
			cameraTransceiver:      true,
		},
	}

	// Envia candidatos ICE (rotas de rede) para o navegador.
	peerConnection.OnICECandidate(func(i *webrtc.ICECandidate) {
		if i == nil {
			return
		}
		candidateJSON, err := json.Marshal(i.ToJSON())
		if err != nil {
			log.Println(err)
			return
		}
		if err := c.WriteJSON(&websocketMessage{Event: "candidate", Data: string(candidateJSON)}); err != nil {
			log.Println(err)
		}
	})

	peerConnection.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		switch s {
		case webrtc.PeerConnectionStateFailed:
			_ = peerConnection.Close()
		case webrtc.PeerConnectionStateClosed:
			signalPeers()
		default:
		}
	})

	// Quando chega mídia de alguém, redistribuímos para os demais.
	peerConnection.OnTrack(func(t *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		kind := kindMic
		switch receiver {
		case micTransceiver.Receiver():
			kind = kindMic
		case screenAudioTransceiver.Receiver():
			kind = kindScreenAudio
		case screenTransceiver.Receiver():
			kind = kindScreen
		case cameraTransceiver.Receiver():
			kind = kindCam
		}

		local, err := publishTrack(p, t, kind)
		if err != nil {
			log.Printf("publicando track %s de %s: %v", kind, p.id, err)
			return
		}
		defer unpublishTrack(local)

		// Vídeo depende de quadros-chave (keyframes) pra decodificar; se o
		// primeiro se perder, quem está assistindo fica com tela preta pra
		// sempre. Pedimos um novo de tempos em tempos, o que garante que
		// qualquer participante (mesmo quem entrou depois) receba um quadro
		// completo em poucos segundos.
		if t.Kind() == webrtc.RTPCodecTypeVideo {
			done := make(chan struct{})
			defer close(done)
			go requestKeyframes(peerConnection, t.SSRC(), done)
		}

		// 1600 bytes cobrem um pacote de MTU cheia com extensões de cabeçalho;
		// com um buffer menor, um pacote grande viraria io.ErrShortBuffer e
		// derrubaria a track inteira.
		buf := make([]byte, 1600)
		for {
			n, _, err := t.Read(buf)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					log.Printf("lendo track %s de %s: %v", kind, p.id, err)
				}
				return
			}
			// Repassa o pacote RTP cru para a track redistribuída.
			if _, err = local.Write(buf[:n]); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				log.Printf("encaminhando track %s de %s: %v", kind, p.id, err)
				return
			}
		}
	})

	if err := joinRoom(p); err != nil {
		log.Printf("registrando participante: %v", err)
		return
	}
	defer leaveRoom(p)

	// Faz uma primeira sincronização para conectar este participante.
	signalPeers()

	// Loop de leitura das mensagens de sinalização vindas do navegador.
	// Erros de sinalização são registrados mas NÃO derrubam a conexão: com
	// várias pessoas entrando e ligando câmera ao mesmo tempo, um candidato
	// fora de ordem é normal e não é motivo pra expulsar ninguém da sala.
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}

		message := &websocketMessage{}
		if err := json.Unmarshal(raw, message); err != nil {
			log.Printf("mensagem inválida de %s: %v", p.id, err)
			continue
		}

		switch message.Event {
		case "candidate":
			candidate := webrtc.ICECandidateInit{}
			if err := json.Unmarshal([]byte(message.Data), &candidate); err != nil {
				log.Printf("candidato inválido de %s: %v", p.id, err)
				continue
			}
			if err := peerConnection.AddICECandidate(candidate); err != nil {
				log.Printf("AddICECandidate (%s): %v", p.id, err)
			}

		case "answer":
			answer := webrtc.SessionDescription{}
			if err := json.Unmarshal([]byte(message.Data), &answer); err != nil {
				log.Printf("resposta inválida de %s: %v", p.id, err)
				continue
			}
			if err := peerConnection.SetRemoteDescription(answer); err != nil {
				log.Printf("SetRemoteDescription (%s): %v", p.id, err)
				continue
			}
			// A conexão acabou de voltar a ficar "estável" (pode receber uma
			// nova oferta). Se algo mudou enquanto esperávamos essa resposta
			// (ex.: a câmera ligou logo depois da tela), syncLocked tinha pulado
			// o envio daquela oferta — agora sim mandamos.
			signalPeers()

		// A troca de vídeo em si (replaceTrack) não passa por aqui. O navegador
		// PEDE permissão antes de capturar a tela, e só começa depois do
		// "screenshare-granted" — assim duas pessoas nunca acabam transmitindo
		// a tela ao mesmo tempo por causa de uma corrida.
		case "screenshare-request":
			roomLock.Lock()
			busy := screenSharer != nil && screenSharer != p
			if !busy {
				screenSharer = p
			}
			roomLock.Unlock()

			if busy {
				_ = c.WriteJSON(&websocketMessage{Event: "screenshare-rejected"})
				continue
			}
			if err := c.WriteJSON(&websocketMessage{Event: "screenshare-granted"}); err != nil {
				releaseScreen(p)
				continue
			}
			broadcastState()

		case "screenshare-stop":
			releaseScreen(p)

		// Câmera e microfone não têm exclusividade: o servidor só guarda e
		// repassa o estado, que é o que diz aos outros navegadores se devem
		// mostrar a miniatura de vídeo dessa pessoa.
		case "camera-state":
			roomLock.Lock()
			p.cameraOn = message.Data == "on"
			roomLock.Unlock()
			broadcastState()

		case "mic-state":
			roomLock.Lock()
			p.muted = message.Data == "muted"
			roomLock.Unlock()
			broadcastState()

		default:
			log.Printf("evento desconhecido de %s: %q", p.id, message.Event)
		}
	}
}

// requestKeyframes pede um quadro-chave periodicamente até a track acabar.
// O canal done é o que impede a goroutine de ficar viva pra sempre depois que
// a pessoa desligou a câmera ou saiu.
func requestKeyframes(pc *webrtc.PeerConnection, ssrc webrtc.SSRC, done <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := pc.WriteRTCP([]rtcp.Packet{
				&rtcp.PictureLossIndication{MediaSSRC: uint32(ssrc)},
			}); err != nil {
				return
			}
		}
	}
}

// nextPeerID dá a cada participante um identificador estável durante a
// conexão. É com ele que o navegador correlaciona tracks, câmera e tela ao
// dono certo — antes isso era inferido do MediaStream, que não sobrevive a
// reconexões.
func nextPeerID() string {
	return "p" + strconv.FormatUint(peerSeq.Add(1), 10)
}
