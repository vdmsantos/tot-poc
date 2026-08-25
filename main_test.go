package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// Estes testes sobem o SFU de verdade e conectam participantes de verdade
// (Pion fazendo o papel do navegador), reproduzindo o cenário que quebrava:
// três pessoas na sala, todas com câmera ligada e uma compartilhando a tela.

// ---------------------------------------------------------------------------
// Infraestrutura do teste
// ---------------------------------------------------------------------------

// resetRoom limpa o estado global entre um teste e outro.
func resetRoom(t *testing.T) {
	t.Helper()
	roomLock.Lock()
	peers = nil
	tracks = map[string]*roomTrack{}
	pendingRenegotiation = map[*peerState]bool{}
	screenSharer = nil
	roomLock.Unlock()
}

func startServer(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", websocketHandler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

// testClient imita o que o navegador faz: reserva um canal por tipo de mídia
// (mic, áudio da tela, vídeo da tela, câmera) e responde às ofertas do SFU.
type testClient struct {
	t    *testing.T
	name string

	pc *webrtc.PeerConnection
	ws *websocket.Conn

	wsMu sync.Mutex

	mic         *webrtc.TrackLocalStaticSample
	screenAudio *webrtc.TrackLocalStaticSample
	screenVideo *webrtc.TrackLocalStaticSample
	camera      *webrtc.TrackLocalStaticSample

	mu       sync.Mutex
	peerID   string
	state    roomState
	gotState bool
	errs     []string

	shareResult  chan string // "granted" | "rejected"
	closed       chan struct{}
	closeOnce    sync.Once
	shuttingDown bool // protegido por mu
}

func newTestClient(t *testing.T, wsURL, name string) *testClient {
	t.Helper()

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("%s: NewPeerConnection: %v", name, err)
	}

	c := &testClient{
		t:           t,
		name:        name,
		pc:          pc,
		shareResult: make(chan string, 4),
		closed:      make(chan struct{}),
	}

	audioCap := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}
	videoCap := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}
	streamID := name + "-stream"

	// A ORDEM importa: é ela que diz ao servidor qual m-line é tela e qual é
	// câmera. Mesma ordem do index.html.
	c.mic = c.addTrack(audioCap, name+"-mic", streamID)
	c.screenAudio = c.addTrack(audioCap, name+"-screen-audio", streamID)
	c.screenVideo = c.addTrack(videoCap, name+"-screen-video", streamID)
	c.camera = c.addTrack(videoCap, name+"-camera", streamID)

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("%s: dial websocket: %v", name, err)
	}
	c.ws = ws

	pc.OnICECandidate(func(i *webrtc.ICECandidate) {
		if i == nil {
			return
		}
		raw, err := json.Marshal(i.ToJSON())
		if err != nil {
			return
		}
		c.send("candidate", string(raw))
	})

	go c.readLoop()

	// Cleanups rodam na ordem inversa: primeiro Close (que encerra o readLoop),
	// depois o despejo dos erros. Assim a goroutine nunca chama t.Errorf com o
	// teste já encerrado.
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, e := range c.errs {
			t.Errorf("%s: %s", c.name, e)
		}
	})
	t.Cleanup(c.Close)
	return c
}

// errf guarda um erro visto dentro do readLoop pra ser reportado no fim do
// teste, na goroutine principal.
func (c *testClient) errf(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shuttingDown {
		return // erro causado pelo próprio encerramento do cliente
	}
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

func (c *testClient) addTrack(cap webrtc.RTPCodecCapability, id, streamID string) *webrtc.TrackLocalStaticSample {
	c.t.Helper()
	track, err := webrtc.NewTrackLocalStaticSample(cap, id, streamID)
	if err != nil {
		c.t.Fatalf("%s: NewTrackLocalStaticSample(%s): %v", c.name, id, err)
	}
	if _, err := c.pc.AddTrack(track); err != nil {
		c.t.Fatalf("%s: AddTrack(%s): %v", c.name, id, err)
	}
	return track
}

func (c *testClient) send(event, data string) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	_ = c.ws.WriteJSON(&websocketMessage{Event: event, Data: data})
}

func (c *testClient) readLoop() {
	for {
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var m websocketMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}

		switch m.Event {
		case "welcome":
			c.mu.Lock()
			c.peerID = m.Data
			c.mu.Unlock()

		case "state":
			var s roomState
			if err := json.Unmarshal([]byte(m.Data), &s); err != nil {
				continue
			}
			c.mu.Lock()
			c.state = s
			c.gotState = true
			c.mu.Unlock()

		case "offer":
			var offer webrtc.SessionDescription
			if err := json.Unmarshal([]byte(m.Data), &offer); err != nil {
				continue
			}
			if err := c.pc.SetRemoteDescription(offer); err != nil {
				c.errf("SetRemoteDescription: %v", err)
				continue
			}
			answer, err := c.pc.CreateAnswer(nil)
			if err != nil {
				c.errf("CreateAnswer: %v", err)
				continue
			}
			if err := c.pc.SetLocalDescription(answer); err != nil {
				c.errf("SetLocalDescription: %v", err)
				continue
			}
			raw, err := json.Marshal(answer)
			if err != nil {
				continue
			}
			c.send("answer", string(raw))

		case "candidate":
			var candidate webrtc.ICECandidateInit
			if err := json.Unmarshal([]byte(m.Data), &candidate); err != nil {
				continue
			}
			_ = c.pc.AddICECandidate(candidate)

		case "screenshare-granted":
			select {
			case c.shareResult <- "granted":
			default:
			}
		case "screenshare-rejected":
			select {
			case c.shareResult <- "rejected":
			default:
			}
		}
	}
}

func (c *testClient) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.shuttingDown = true
		c.mu.Unlock()

		close(c.closed)
		_ = c.ws.Close()
		_ = c.pc.Close()
	})
}

func (c *testClient) id() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerID
}

func (c *testClient) snapshot() roomState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// stream começa a transmitir numa track até o cliente fechar. Sem isso o
// servidor nunca vê a mídia (é o pacote RTP que dispara o OnTrack).
func (c *testClient) stream(track *webrtc.TrackLocalStaticSample, every time.Duration) {
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		payload := make([]byte, 200)
		for {
			select {
			case <-c.closed:
				return
			case <-ticker.C:
				_ = track.WriteSample(media.Sample{Data: payload, Duration: every})
			}
		}
	}()
}

// waitFor espera uma condição virar verdade, com uma mensagem útil se estourar.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = cond(); last == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout esperando %s: %v", what, last)
}

// kindsDe devolve os tipos de mídia que o retrato da sala atribui a um peer.
func kindsDe(s roomState, peerID string) map[string]bool {
	out := map[string]bool{}
	for _, tr := range s.Tracks {
		if tr.PeerID == peerID {
			out[tr.Kind] = true
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Testes
// ---------------------------------------------------------------------------

// TestTresParticipantesComCameraETela é o cenário que quebrava: três pessoas
// entram, todas ligam a câmera e uma compartilha a tela. Todo mundo tem que
// enxergar a tela e a câmera de todo mundo — inclusive quem entrou por último.
func TestTresParticipantesComCameraETela(t *testing.T) {
	resetRoom(t)
	wsURL := startServer(t)

	clients := make([]*testClient, 0, 3)
	for i := 1; i <= 3; i++ {
		c := newTestClient(t, wsURL, fmt.Sprintf("peer%d", i))
		clients = append(clients, c)

		// Cada um liga microfone e câmera assim que entra — e o terceiro entra
		// bem depois, que é justamente o caso em que o cliente antigo perdia os
		// avisos de "essa track é uma câmera".
		c.stream(c.mic, 20*time.Millisecond)
		c.stream(c.camera, 33*time.Millisecond)

		waitFor(t, 20*time.Second, fmt.Sprintf("%s receber seu peerId", c.name), func() error {
			if c.id() == "" {
				return fmt.Errorf("sem welcome")
			}
			return nil
		})
	}

	// O primeiro compartilha a tela (com áudio).
	sharer := clients[0]
	sharer.send("screenshare-request", "")
	select {
	case res := <-sharer.shareResult:
		if res != "granted" {
			t.Fatalf("esperava permissão pra compartilhar, veio %q", res)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout esperando resposta do screenshare-request")
	}
	sharer.stream(sharer.screenVideo, 33*time.Millisecond)
	sharer.stream(sharer.screenAudio, 20*time.Millisecond)

	for _, c := range clients {
		c.send("camera-state", "on")
	}

	// Todo mundo tem que ver o mesmo retrato da sala: 3 participantes, todos
	// com câmera, e o peer1 compartilhando tela e áudio de tela.
	for _, c := range clients {
		c := c
		waitFor(t, 25*time.Second, c.name+" enxergar a sala inteira", func() error {
			s := c.snapshot()
			if len(s.Peers) != 3 {
				return fmt.Errorf("%d participantes, esperava 3", len(s.Peers))
			}
			for _, p := range s.Peers {
				if !p.CameraOn {
					return fmt.Errorf("%s ainda sem cameraOn", p.PeerID)
				}
			}
			if s.ScreenSharer != sharer.id() {
				return fmt.Errorf("screenSharer=%q, esperava %q", s.ScreenSharer, sharer.id())
			}
			for _, other := range clients {
				want := map[string]bool{kindMic: true, kindCam: true}
				if other == sharer {
					want[kindScreen] = true
					want[kindScreenAudio] = true
				}
				got := kindsDe(s, other.id())
				for kind := range want {
					if !got[kind] {
						return fmt.Errorf("faltando %q de %s (tem %v)", kind, other.name, got)
					}
				}
			}
			return nil
		})
	}

	// E cada um tem que estar REALMENTE recebendo a mídia dos outros dois
	// (4 tracks do apresentador + 2 de cada um dos outros), sem receber a
	// própria de volta.
	for _, c := range clients {
		c := c
		esperado := 0
		for _, other := range clients {
			if other == c {
				continue
			}
			if other == sharer {
				esperado += 4
			} else {
				esperado += 2
			}
		}
		waitFor(t, 30*time.Second, c.name+" receber a mídia dos outros", func() error {
			recebidas := 0
			for _, receiver := range c.pc.GetReceivers() {
				if tr := receiver.Track(); tr != nil && tr.ID() != "" &&
					!strings.HasPrefix(tr.ID(), c.name+"-") {
					recebidas++
				}
			}
			if recebidas < esperado {
				return fmt.Errorf("%d tracks recebidas, esperava %d", recebidas, esperado)
			}
			for _, receiver := range c.pc.GetReceivers() {
				if tr := receiver.Track(); tr != nil && strings.HasPrefix(tr.ID(), c.name+"-") {
					return fmt.Errorf("recebeu a própria track %q de volta", tr.ID())
				}
			}
			return nil
		})
	}
}

// TestApenasUmaTelaPorVez garante que o segundo pedido de compartilhamento é
// recusado ANTES de qualquer captura, e que a vez volta pra sala quando quem
// estava compartilhando para.
func TestApenasUmaTelaPorVez(t *testing.T) {
	resetRoom(t)
	wsURL := startServer(t)

	a := newTestClient(t, wsURL, "peerA")
	b := newTestClient(t, wsURL, "peerB")
	for _, c := range []*testClient{a, b} {
		c := c
		waitFor(t, 20*time.Second, c.name+" entrar", func() error {
			if c.id() == "" {
				return fmt.Errorf("sem welcome")
			}
			return nil
		})
	}

	a.send("screenshare-request", "")
	if res := <-a.shareResult; res != "granted" {
		t.Fatalf("peerA: esperava granted, veio %q", res)
	}

	b.send("screenshare-request", "")
	select {
	case res := <-b.shareResult:
		if res != "rejected" {
			t.Fatalf("peerB: esperava rejected enquanto peerA compartilha, veio %q", res)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("peerB não recebeu resposta do pedido de compartilhamento")
	}

	a.send("screenshare-stop", "")
	waitFor(t, 10*time.Second, "a sala saber que ninguém está compartilhando", func() error {
		if s := b.snapshot(); s.ScreenSharer != "" {
			return fmt.Errorf("screenSharer=%q", s.ScreenSharer)
		}
		return nil
	})

	b.send("screenshare-request", "")
	select {
	case res := <-b.shareResult:
		if res != "granted" {
			t.Fatalf("peerB: esperava granted depois que peerA parou, veio %q", res)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("peerB não recebeu resposta do segundo pedido")
	}
}

// TestSaidaLimpaAMidia garante que quando alguém sai, a câmera dessa pessoa
// some do retrato da sala dos outros — antes ficava um quadradinho congelado
// pra sempre, porque o navegador nunca marca a track como "ended".
func TestSaidaLimpaAMidia(t *testing.T) {
	resetRoom(t)
	wsURL := startServer(t)

	a := newTestClient(t, wsURL, "peerA")
	b := newTestClient(t, wsURL, "peerB")
	b.stream(b.mic, 20*time.Millisecond)
	b.stream(b.camera, 33*time.Millisecond)

	waitFor(t, 25*time.Second, "peerA ver a câmera de peerB", func() error {
		s := a.snapshot()
		if len(s.Peers) != 2 {
			return fmt.Errorf("%d participantes", len(s.Peers))
		}
		if !kindsDe(s, b.id())[kindCam] {
			return fmt.Errorf("câmera de peerB ainda não apareceu")
		}
		return nil
	})

	bID := b.id()
	b.Close()

	waitFor(t, 20*time.Second, "a mídia de peerB sumir para peerA", func() error {
		s := a.snapshot()
		if len(s.Peers) != 1 {
			return fmt.Errorf("%d participantes, esperava 1", len(s.Peers))
		}
		if len(kindsDe(s, bID)) != 0 {
			return fmt.Errorf("ainda sobrou mídia de peerB: %v", kindsDe(s, bID))
		}
		return nil
	})
}
