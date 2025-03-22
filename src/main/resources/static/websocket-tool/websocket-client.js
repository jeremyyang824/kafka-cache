class WebSocketClient {
  constructor(url, options = {}) {
    this.url = url;
    this.options = {
      heartbeatInterval: options.heartbeatInterval || 30000
    };
    
    this.ws = null;
    this.heartbeatTimer = null;
    this.eventHandlers = {
      connected: [],
      disconnected: [],
      error: [],
      message: []
    };
  }

  connect() {
    try {
      if (this.ws) {
        this.ws.close();
      }

      this.ws = new WebSocket(this.url);
      
      this.ws.onopen = () => {
        this.startHeartbeat();
        this.emit('connected');
      };

      this.ws.onclose = () => {
        this.stopHeartbeat();
        this.emit('disconnected');
      };

      this.ws.onerror = (error) => {
        this.emit('error', error);
      };

      this.ws.onmessage = (event) => {
        let message = event.data;
        try {
          message = JSON.parse(event.data);
        } catch (e) {
          // 保持原始消息格式
        }
        
        if (typeof this.handleMessage === 'function') {
          this.handleMessage(message);
        }
        this.emit('message', message);
      };
    } catch (error) {
      this.emit('error', error);
    }
  }

  startHeartbeat() {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.send('heartbeat', { timestamp: Date.now() });
      }
    }, this.options.heartbeatInterval);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  send(type, data) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not connected');
    }

    const message = {
      type,
      data,
      timestamp: Date.now()
    };

    try {
      this.ws.send(JSON.stringify(message));
    } catch (error) {
      this.emit('error', new Error('Failed to send message: ' + error.message));
      throw error;
    }
  }

  close() {
    this.stopHeartbeat();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  on(event, callback) {
    if (this.eventHandlers[event]) {
      this.eventHandlers[event].push(callback);
    }
  }

  off(event, callback) {
    if (this.eventHandlers[event]) {
      this.eventHandlers[event] = this.eventHandlers[event].filter(cb => cb !== callback);
    }
  }

  emit(event, data) {
    if (this.eventHandlers[event]) {
      this.eventHandlers[event].forEach(callback => callback(data));
    }
  }
}
