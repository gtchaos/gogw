package main

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/soheilhy/cmux"
)

func main() {
	// 1. 创建主监听器，监听端口 (例如，和frps共用7000端口，或自定义端口)
	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatal("Failed to listen on :7000:", err)
	}
	defer listener.Close()
	log.Println("CMUX dispatcher started on :7000")

	// 2. 创建 CMUX 多路复用器
	mux := cmux.New(listener)

	// 3. 定义匹配规则
	// 匹配所有HTTP 1.x流量
	httpListener := mux.Match(cmux.HTTP1Fast())
	// 匹配所有其他流量 (将转发给frps)
	tcpListener := mux.Match(cmux.Any())

	// 4. 启动HTTP处理goroutine (可以转发给其他WebServer)
	go serveHTTP(httpListener)
	// 5. 启动TCP处理goroutine (转发给frps)
	go serveTCPToFRPS(tcpListener)

	// 6. 开始多路复用服务
	log.Println("Starting cmux...")
	if err = mux.Serve(); err != nil {
		log.Fatal("CMUX serve error:", err)
	}
}

// serveHTTP 处理HTTP流量（示例：可以转发到其他后端）
func serveHTTP(l net.Listener) {
	// 1. 创建强大的路由器
	router := mux.NewRouter()

	// 2. 创建反向代理到其他WebServer（例如Nginx、静态文件服务）
	targetURL, _ := url.Parse("http://59.110.10.92:8080") // 你的其他WebServer
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// 3. 定义路由规则
	// 规则1：所有以 /api/ 开头的请求，由本地Go函数处理
	apiRouter := router.PathPrefix("/api/v1").Subrouter()
	apiRouter.HandleFunc("/users", getUsersHandler).Methods("GET")

	// 规则2：所有以 /admin/ 开头的请求，也由本地处理，但可能需要中间件
	adminRouter := router.PathPrefix("/admin/").Subrouter()
	adminRouter.Use(adminAuthMiddleware) // 认证中间件
	adminRouter.HandleFunc("/dashboard", adminDashboardHandler)

	// 规则3：所有其他请求（如 /, /static/, /about.html），全部反向代理到其他WebServer
	router.PathPrefix("/openapi/v1").Handler(proxy)

	// 创建一个HTTP服务器，它的唯一 handler 就是这个反向代理
	httpServer := &http.Server{
		Handler: router,
		// 设置一些超时时间以避免连接僵死
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("HTTP reverse proxy started")
	// 使用我们自己的Listener来服务，而不是用默认的
	if err := httpServer.Serve(l); err != nil {
		// 如果不是因为Listener关闭导致的错误，则记录日志
		log.Printf("HTTP server error: %v", err)
	}
}

// --- 中间件示例 ---
func adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "secret-token" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func getUsersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// 这里可以从数据库获取数据并返回JSON
	w.Write([]byte(`[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`))
}

func adminDashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Welcome to the admin dashboard!"))
}

// serveTCPToFRPS 处理原始TCP流量，并将其转发到frps的特定端口
func serveTCPToFRPS(l net.Listener) {
	// frps 的地址和端口
	// 假设你的 frps 运行在同一台机器的 7000 端口
	frpsTarget := "127.0.0.1:7000"
	// 如果你的 frps 在其他机器，例如：frpsTarget := "frps.your-domain.com:7000"

	log.Printf("TCP-to-FRPS proxy started, forwarding to %s", frpsTarget)

	for {
		clientConn, err := l.Accept()
		if err != nil {

			if errors.Is(err, cmux.ErrListenerClosed) {
				log.Println("TCP listener closed (expected)")
				return
			}
			log.Printf("TCP accept error: %v", err)
			continue
		}

		// 为每个TCP连接启动一个goroutine，将其转发到frps
		go proxyTCPConnectionToFRPS(clientConn, frpsTarget)
	}
}

// proxyTCPConnectionToFRPS 核心函数：将客户端连接代理到frps
func proxyTCPConnectionToFRPS(clientConn net.Conn, frpsAddr string) {
	defer clientConn.Close()

	// 1. 连接到目标 frps
	frpsConn, err := net.Dial("tcp", frpsAddr)
	if err != nil {
		log.Printf("Failed to connect to FRPS [%s]: %v", frpsAddr, err)
		return
	}
	defer frpsConn.Close()

	remoteAddr := clientConn.RemoteAddr().String()
	log.Printf("Proxying TCP connection from %s to FRPS [%s]", remoteAddr, frpsAddr)

	// 2. 进行双向转发
	// 从客户端读，写到frps
	go func() {
		io.Copy(frpsConn, clientConn)
		// 如果一端关闭，通知另一端也关闭
		clientConn.Close()
		frpsConn.Close()
	}()

	// 从frps读，写到客户端
	io.Copy(clientConn, frpsConn)
	// 如果一端关闭，通知另一端也关闭
	clientConn.Close()
	frpsConn.Close()

	log.Printf("TCP connection from %s to FRPS closed", remoteAddr)
}
