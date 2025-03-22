class WebSocketDebugger {
  constructor() {
    this.wsClient = null;
    this.messageHistory = [];
    this.stats = {
      sent: 0,
      received: 0,
      bytesSent: 0,
      bytesReceived: 0
    };

    // DOM elements
    this.elements = {
      connectBtn: document.getElementById('connectBtn'),
      wsUrl: document.getElementById('wsUrl'),
      messageInput: document.getElementById('messageInput'),
      sendBtn: document.getElementById('sendBtn'),
      responseArea: document.getElementById('responseArea'),
      connectionStatus: document.getElementById('connectionStatus'),
      sentCount: document.getElementById('sentCount'),
      receivedCount: document.getElementById('receivedCount'),
      totalBytes: document.getElementById('totalBytes'),
      requestPanel: document.getElementById('requestPanel'),
      requestPanelHeader: document.getElementById('requestPanelHeader'),
      statusIndicator: document.getElementById('statusIndicator'),
      exportBtn: document.getElementById('exportBtn'),
      clearBtn: document.getElementById('clearBtn')
    };

    this.initializeEventListeners();
  }

  initializeEventListeners() {
    this.elements.connectBtn.addEventListener('click', () => this.handleConnection());
    this.elements.sendBtn.addEventListener('click', () => this.handleSendMessage());
    this.elements.exportBtn.addEventListener('click', () => this.exportToCSV());
    this.elements.clearBtn.addEventListener('click', () => this.clearMessages());
    this.elements.requestPanelHeader.addEventListener('click', () => this.togglePanel());
    this.elements.messageInput.addEventListener('keydown', (e) => this.handleKeyPress(e));
  }

  handleKeyPress(e) {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      this.elements.sendBtn.click();
    }
  }

  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  updateStats() {
    this.elements.sentCount.textContent = this.stats.sent;
    this.elements.receivedCount.textContent = this.stats.received;
    this.elements.totalBytes.textContent = this.formatBytes(this.stats.bytesSent + this.stats.bytesReceived);
  }

  updateConnectionStatus(connected) {
    this.elements.connectionStatus.textContent = connected ? 'Connected' : 'Disconnected';
    this.elements.connectionStatus.style.color = connected ? '#10b981' : '#ef4444';
    this.elements.connectBtn.textContent = connected ? 'Disconnect' : 'Connect';
    this.elements.connectBtn.classList.toggle('connected', connected);
    this.elements.statusIndicator.classList.toggle('connected', connected);
  }

  togglePanel(collapse) {
    if (collapse === undefined) {
      this.elements.requestPanel.classList.toggle('collapsed');
    } else {
      this.elements.requestPanel.classList.toggle('collapsed', collapse);
    }
  }

  addMessage(message, direction = 'incoming', isError = false) {
    const time = new Date().toLocaleString();
    const content = typeof message === 'string' ? message : JSON.stringify(message);
    
    this.messageHistory.push({
      time,
      direction,
      isError,
      content
    });

    const messageEl = document.createElement('div');
    messageEl.className = `message-item${isError ? ' message-error' : ''}`;

    const directionSpan = document.createElement('span');
    directionSpan.className = `message-direction message-${direction}`;
    directionSpan.textContent = direction === 'incoming' ? 'Recv' : 'Sent';
    
    const timeEl = document.createElement('span');
    timeEl.className = 'message-time';
    timeEl.textContent = time;

    const contentEl = document.createElement('div');
    contentEl.className = 'message-content';
    contentEl.textContent = typeof message === 'string' ? message : 
      JSON.stringify(message, null, 2);

    messageEl.append(directionSpan, timeEl, contentEl);
    this.elements.responseArea.appendChild(messageEl);
    this.elements.responseArea.scrollTop = this.elements.responseArea.scrollHeight;
  }

  handleConnection() {
    if (this.wsClient?.ws?.readyState === WebSocket.OPEN) {
      this.wsClient.close();
      this.stats = { sent: 0, received: 0, bytesSent: 0, bytesReceived: 0 };
      this.updateStats();
      this.updateConnectionStatus(false);
      this.addMessage('Connection closed', 'incoming');
      return;
    }

    const url = this.elements.wsUrl.value.trim();
    if (!url) {
      this.addMessage('Please enter a WebSocket URL', 'incoming', true);
      return;
    }

    if (!url.startsWith('ws://') && !url.startsWith('wss://')) {
      this.addMessage('Invalid WebSocket URL. Must start with ws:// or wss://', 'incoming', true);
      return;
    }

    this.wsClient = new WebSocketClient(url, { heartbeatInterval: 30000 });

    this.wsClient.handleMessage = (message) => {
      this.stats.received++;
      const messageStr = typeof message === 'string' ? message : JSON.stringify(message);
      this.stats.bytesReceived += new Blob([messageStr]).size;
      this.updateStats();
      this.addMessage(message, 'incoming');
    };

    this.wsClient.on('connected', () => {
      this.updateConnectionStatus(true);
      this.addMessage('Connected to WebSocket server', 'incoming');
    });

    this.wsClient.on('disconnected', () => {
      this.updateConnectionStatus(false);
      this.addMessage('Disconnected from WebSocket server', 'incoming');
    });

    this.wsClient.on('error', (error) => {
      this.addMessage(error.message || 'Connection error', 'incoming', true);
    });

    this.wsClient.connect();
  }

  handleSendMessage() {
    if (!this.wsClient?.ws?.readyState === WebSocket.OPEN) {
      this.addMessage('Please connect to WebSocket server first', 'incoming', true);
      return;
    }

    const messageText = this.elements.messageInput.value.trim();
    if (!messageText) {
      this.addMessage('Please enter a message to send', 'incoming', true);
      return;
    }

    try {
      this.wsClient.send('message', messageText);
      this.stats.sent++;
      this.stats.bytesSent += new Blob([messageText]).size;
      this.updateStats();
      this.addMessage(messageText, 'outgoing');
      this.togglePanel(true);
    } catch (error) {
      this.addMessage('Failed to send message: ' + error.message, 'incoming', true);
    }
  }

  exportToCSV() {
    if (this.messageHistory.length === 0) {
      this.addMessage('No messages to export', 'incoming', true);
      return;
    }

    const csv = [
      'Time,Direction,Type,Content',
      ...this.messageHistory.map(msg => {
        const direction = msg.direction === 'outgoing' ? 'Sent' : 'Received';
        const type = msg.isError ? 'Error' : 'Message';
        const content = msg.content.replace(/"/g, '""');
        return `${msg.time},${direction},${type},"${content}"`;
      })
    ].join('\n');

    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `websocket_messages_${new Date().toISOString().slice(0,19).replace(/[:]/g, '-')}.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  clearMessages() {
    if (this.messageHistory.length === 0) return;
    
    if (confirm('Are you sure you want to clear all messages?')) {
      this.messageHistory = [];
      this.elements.responseArea.innerHTML = '';
    }
  }
}

// Initialize the application
const app = new WebSocketDebugger(); 