package client

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"gogw/common"
	"gogw/logger"
	"gogw/schema"
)

const (
	PACKSIZE = 1024 * 1024
)

type Client struct {
	ServerAddr  string
	SourceAddr  string
	ToPort      int
	Direction   string
	Protocol    string
	Description string
	Compress    bool
	ClientId    string
	HttpVersion string

	Conns           *sync.Map
	UDPAddrToConnId map[string]string
}

func NewClient(
	serverAddr string,
	sourceAddr string,
	toPort int,
	direction string,
	protocol string,
	description string,
	compress bool,
	httpVersion string,
) *Client {
	return &Client{
		ServerAddr:      serverAddr,
		SourceAddr:      sourceAddr,
		ToPort:          toPort,
		Direction:       direction,
		Protocol:        protocol,
		Description:     description,
		Compress:        compress,
		HttpVersion:     httpVersion,
		ClientId:        "",
		Conns:           &sync.Map{},
		UDPAddrToConnId: make(map[string]string),
	}
}

func (c *Client) Start() {
	logger.Info(fmt.Sprintf("\nclient start\nServer: %v\nSourceAddr: %v\nToPort: %v\nDirection: %v\nProtocol: %v\nDescription: %v\nCompress: %v\nHttpVersion: %v\n",
		c.ServerAddr, c.SourceAddr, c.ToPort, c.Direction, c.Protocol, c.Description, c.Compress, c.HttpVersion))

	//start heartbeat
	go c.heartbeatLoop()

	for {
		if err := c.register(); err != nil {
			logger.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}

		if c.Direction == schema.DIRECTION_FORWARD {
			if c.Protocol == schema.PROTOCOL_TCP {
				c.startForwardTCPListener()
			}

			if c.Protocol == schema.PROTOCOL_UDP {
				c.startForwardUDPListener()
			}
		}

		if err := c.msgRequestLoop(); err != nil {
			logger.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
	}
}

func (c *Client) heartbeatLoop() {
	for {
		func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Warn(err)
				}
			}()

			if c.ClientId != "" {
				url := fmt.Sprintf("http://%s/heartbeat?clientid=%s", c.ServerAddr, c.ClientId)
				resp, err := http.Get(url)

				if err != nil {
					logger.Error(err)
					return
				}
				defer resp.Body.Close()

			}
		}()

		time.Sleep(3 * time.Second)
	}
}

func (c *Client) register() error {
	logger.Info("start register")
	url := fmt.Sprintf("http://%v/register", c.ServerAddr)
	msgPack := &schema.MsgPack{
		MsgType: schema.MSG_TYPE_REGISTER_REQUEST,
		Msg: &schema.RegisterRequest{
			SourceAddr:  c.SourceAddr,
			ToPort:      c.ToPort,
			Direction:   c.Direction,
			Protocol:    c.Protocol,
			Description: c.Description,
			Compress:    c.Compress,
			HttpVersion: c.HttpVersion,
		},
	}

	r, w := io.Pipe()
	go func() {
		schema.WriteMsg(w, msgPack)
		w.Close()
	}()

	response, err := http.Post(url, "", r)
	if err != nil {
		return fmt.Errorf("register request fail, %v", err)
	}

	msgPack, err = schema.ReadMsg(response.Body)
	if err != nil {
		return fmt.Errorf("read response fail when register, %v", err)
	}

	msg, _ := msgPack.Msg.(*schema.RegisterResponse)
	if msg.Status == schema.STATUS_SUCCESS {
		c.ClientId = msg.ClientId
	} else {
		err = fmt.Errorf("register failed")
	}

	return err
}

func (c *Client) msgRequestLoop() error {
	url := fmt.Sprintf("http://%v/msg?clientid=%v", c.ServerAddr, c.ClientId)
	msgPack := &schema.MsgPack{
		MsgType: schema.MSG_TYPE_MSG_COMMON_REQUEST,
	}

	var buf bytes.Buffer
	schema.WriteMsg(&buf, msgPack)
	data := buf.Bytes()

	for {
		response, err := http.Post(url, "", bytes.NewReader(data))
		if err != nil {
			logger.Error(err)
			return fmt.Errorf("msg request fail, %v", err)
		}

		msgPackResponse, err := schema.ReadMsg(response.Body)
		if err != nil {
			logger.Error(err)
			return fmt.Errorf("read response fail when loop msg, %v", err)
		}

		response.Body.Close()
		if msgPackResponse.MsgType == schema.MSG_TYPE_OPEN_CONN_RESPONSE {
			msg := msgPackResponse.Msg.(*schema.OpenConnResponse)
			//only reverse connection need this
			if c.Direction == schema.DIRECTION_REVERSE {
				c.openReverseConn(msg.ConnId)
				logger.Info("New Reverse Connection\nClientId: %v\nSourceAddr: %v\nRemoteAddr: %v\n")
			}
		}
	}
}

func (c *Client) openConn(connId string, conn net.Conn) error {
	url := fmt.Sprintf("http://%v/msg?clientid=%v", c.ServerAddr, c.ClientId)
	c.Conns.Store(connId, &common.Conn{
		ConnId: connId,
		Conn:   conn,
	})

	//conn -> server
	go func() {
		defer func() {
			c.closeConn(connId)
		}()

		readerMsgPack := &schema.MsgPack{
			MsgType: schema.MSG_TYPE_OPEN_CONN_REQUEST,
			Msg: &schema.OpenConnRequest{
				ConnId:   connId,
				Role:     schema.ROLE_READER,
				Operator: schema.OPERATOR_DATA_TRANSFER,
			},
		}

		if c.HttpVersion == schema.HTTP_VERSION_1_1 {
			r, w := io.Pipe()
			go func() {
				schema.WriteMsg(w, readerMsgPack)
				common.Copy(w, conn, c.Compress, false, nil)
				w.Close()
			}()

			_, err := http.Post(url, "", r)

			logger.Info("[role_reader] conn to server copy done, error: %v", err)

		}
	}()

	//server -> conn
	go func() {
		defer func() {
			c.closeConn(connId)
		}()

		writerMsgPack := &schema.MsgPack{
			MsgType: schema.MSG_TYPE_OPEN_CONN_REQUEST,
			Msg: &schema.OpenConnRequest{
				ConnId:   connId,
				Role:     schema.ROLE_WRITER,
				Operator: schema.OPERATOR_DATA_TRANSFER,
			},
		}

		if c.HttpVersion == schema.HTTP_VERSION_1_1 {
			r, w := io.Pipe()
			go func() {
				schema.WriteMsg(w, writerMsgPack)
				w.Close()
			}()

			response, err := http.Post(url, "", r)
			if err != nil {
				logger.Error("[role_writer] http post failed, %v", err)
				return
			}

			common.Copy(conn, response.Body, false, c.Compress, nil)
			logger.Info("[role_writer] server to conn copy done")
			response.Body.Close()
		}
	}()

	return nil
}

// only used in HTTP1.0
func (c *Client) closeConn(connId string) {
	url := fmt.Sprintf("http://%v/msg?clientid=%v", c.ServerAddr, c.ClientId)
	readerMsgPack := &schema.MsgPack{
		MsgType: schema.MSG_TYPE_OPEN_CONN_REQUEST,
		Msg: &schema.OpenConnRequest{
			ConnId:   connId,
			Role:     schema.ROLE_READER,
			Operator: schema.OPERATOR_CONN_CLOSE,
		},
	}
	r, w := io.Pipe()
	go func() {
		schema.WriteMsg(w, readerMsgPack)
		w.Close()
	}()

	http.Post(url, "", r)

	readerMsgPack = &schema.MsgPack{
		MsgType: schema.MSG_TYPE_OPEN_CONN_REQUEST,
		Msg: &schema.OpenConnRequest{
			ConnId:   connId,
			Role:     schema.ROLE_WRITER,
			Operator: schema.OPERATOR_CONN_CLOSE,
		},
	}
	r, w = io.Pipe()
	go func() {
		schema.WriteMsg(w, readerMsgPack)
		w.Close()
	}()

	http.Post(url, "", r)
}

func (c *Client) openReverseConn(connId string) error {
	var conn net.Conn
	var err error
	conn, err = net.Dial(c.Protocol, c.SourceAddr)
	if err != nil {
		return err
	}

	c.openConn(connId, conn)
	return nil
}

func (c *Client) startForwardTCPListener() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", c.ToPort))
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logger.Error(err)
				return
			}

			if connId, err := c.queryConnId(); err == nil {
				c.openConn(connId, conn)

				logger.Info(fmt.Sprintf("New Connection\nClientId: %v\nSourceAddr: %v\nRemoteAddr: %v\n",
					c.ClientId, c.SourceAddr, conn.RemoteAddr()))

			} else {
				logger.Error(err)
			}
		}
	}()

	return nil
}

func (c *Client) startForwardUDPListener() error {
	listener, err := net.ListenUDP(c.Protocol, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: c.ToPort})
	if err != nil {
		return err
	}

	go func() {
		bs := make([]byte, PACKSIZE)
		for {
			n, remoteAddr, err := listener.ReadFromUDP(bs)
			if err != nil {
				return
			}

			addr := remoteAddr.String()
			if connId, ok := c.UDPAddrToConnId[addr]; !ok {
				if connId, err = c.queryConnId(); err == nil {
					c.UDPAddrToConnId[addr] = connId
					conn := common.NewUDPConn(remoteAddr, listener)
					c.openConn(connId, conn)

				} else {
					logger.Error(err)
				}
			}

			if value, ok := c.Conns.Load(c.UDPAddrToConnId[addr]); ok {
				conn := value.(*common.Conn)
				udpConn, _ := conn.Conn.(*common.UDPConn)
				udpConn.PipeWriter.Write(bs[:n])
			}

			logger.Info(fmt.Sprintf("New Connection\nClientId: %v\nSourceAddr: %v\nRemoteAddr: %v\n",
				c.ClientId, c.SourceAddr, addr))
		}
	}()

	return nil
}

func (c *Client) queryConnId() (string, error) {
	url := fmt.Sprintf("http://%v/msg?clientid=%v", c.ServerAddr, c.ClientId)
	msgPack := &schema.MsgPack{
		MsgType: schema.MSG_TYPE_OPEN_CONN_REQUEST,
		Msg: &schema.OpenConnRequest{
			Role: schema.ROLE_QUERY_CONNID,
		},
	}

	r, w := io.Pipe()
	go func() {
		schema.WriteMsg(w, msgPack)
		w.Close()
	}()

	response, err := http.Post(url, "", r)
	if err != nil {
		return "", err
	}

	msgPack, err = schema.ReadMsg(response.Body)
	if err != nil || msgPack.MsgType != schema.MSG_TYPE_OPEN_CONN_RESPONSE {
		return "", err
	}

	msg, ok := msgPack.Msg.(*schema.OpenConnResponse)
	if !ok || msg.Status != schema.STATUS_SUCCESS {
		err = fmt.Errorf("query id error")
		return "", err
	}

	return msg.ConnId, nil
}
