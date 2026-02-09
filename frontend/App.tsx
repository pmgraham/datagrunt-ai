import React, { useState, useCallback, useRef, useEffect } from 'react';
import { Header } from './components/Header';
import { ChatContainer } from './components/ChatContainer';
import { Canvas } from './components/Canvas';
import { ChatMessage, SessionState, ToolStatus, CanvasState } from './types';
import {
  uploadFile,
  createSession,
  streamAgentMessage,
  extractCleanedFilePath,
} from './services/adkService';
import { createSampleFile } from './components/ChatInput';
import { isReportMessage } from './utils';

let messageIdCounter = 0;
function nextId(): string {
  return `msg-${++messageIdCounter}`;
}

function App() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [session, setSession] = useState<SessionState | null>(null);
  const [isAgentRunning, setIsAgentRunning] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [canvas, setCanvas] = useState<CanvasState | null>(null);
  const agentMessageIdRef = useRef<string | null>(null);

  const openCanvas = useCallback(() => {
    setCanvas({ isOpen: true });
  }, []);

  const closeCanvas = useCallback(() => {
    setCanvas(null);
  }, []);

  const appendAgentText = useCallback((id: string, token: string) => {
    setMessages((prev) =>
      prev.map((m) => (m.id === id ? { ...m, text: m.text + token } : m)),
    );
  }, []);

  const appendToolCall = useCallback((id: string, tool: ToolStatus) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === id ? { ...m, toolCalls: [...m.toolCalls, tool] } : m,
      ),
    );
  }, []);

  const setCleanedFile = useCallback((id: string, filePath: string) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === id
          ? { ...m, downloadableFile: { path: filePath, name: filePath.split('/').pop() || 'cleaned.csv' } }
          : m,
      ),
    );
  }, []);

  const finalizeAgentMessage = useCallback((id: string, fullText: string) => {
    // Also try regex as fallback if onCleanedFile didn't fire
    const cleanedPath = extractCleanedFilePath(fullText);
    setMessages((prev) =>
      prev.map((m) => {
        if (m.id !== id) return m;
        return {
          ...m,
          text: fullText,
          isStreaming: false,
          downloadableFile: m.downloadableFile
            || (cleanedPath ? { path: cleanedPath, name: cleanedPath.split('/').pop() || 'cleaned.csv' } : undefined),
        };
      }),
    );

    // Auto-open canvas only for report-length messages
    if (fullText && isReportMessage(fullText)) {
      setCanvas({ isOpen: true });
    }
  }, []);

  const markAgentError = useCallback((id: string, error: Error) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === id
          ? { ...m, text: m.text + `\n\n**Error:** ${error.message}`, isStreaming: false }
          : m,
      ),
    );
  }, []);

  const streamResponse = useCallback(
    (sessionId: string, userMessage: string, agentMsgId: string) => {
      // Use rAF batching for token updates
      let pendingTokens = '';
      let rafHandle: number | null = null;

      const flushTokens = () => {
        if (pendingTokens) {
          const batch = pendingTokens;
          pendingTokens = '';
          appendAgentText(agentMsgId, batch);
        }
        rafHandle = null;
      };

      streamAgentMessage(sessionId, userMessage, {
        onToken: (token) => {
          pendingTokens += token;
          if (!rafHandle) {
            rafHandle = requestAnimationFrame(flushTokens);
          }
        },
        onToolCall: (name, friendlyName) => {
          appendToolCall(agentMsgId, { name, friendlyName });
        },
        onCleanedFile: (filePath) => {
          setCleanedFile(agentMsgId, filePath);
        },
        onComplete: (fullText) => {
          if (rafHandle) {
            cancelAnimationFrame(rafHandle);
          }
          finalizeAgentMessage(agentMsgId, fullText);
          setIsAgentRunning(false);
          agentMessageIdRef.current = null;
        },
        onError: (error) => {
          if (rafHandle) {
            cancelAnimationFrame(rafHandle);
          }
          markAgentError(agentMsgId, error);
          setIsAgentRunning(false);
          agentMessageIdRef.current = null;
        },
      });
    },
    [appendAgentText, appendToolCall, setCleanedFile, finalizeAgentMessage, markAgentError],
  );

  const handleFileUpload = useCallback(
    async (file: File) => {
      if (isAgentRunning) return;
      setIsAgentRunning(true);

      // Add user message with file attachment
      const userMsgId = nextId();
      const userMsg: ChatMessage = {
        id: userMsgId,
        role: 'user',
        text: '',
        toolCalls: [],
        fileAttachment: { name: file.name, size: file.size, file },
        isStreaming: false,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, userMsg]);

      try {
        setUploadProgress(0);
        const upload = await uploadFile(file, (pct) => setUploadProgress(pct));
        setUploadProgress(null);
        const sessionId = await createSession();
        const newSession: SessionState = {
          id: sessionId,
          filePath: upload.filePath,
          fileName: file.name,
          rowCount: upload.rowCount,
        };
        setSession(newSession);

        // Add empty agent message
        const agentMsgId = nextId();
        const agentMsg: ChatMessage = {
          id: agentMsgId,
          role: 'agent',
          text: '',
          toolCalls: [],
          isStreaming: true,
          timestamp: Date.now(),
        };
        setMessages((prev) => [...prev, agentMsg]);
        agentMessageIdRef.current = agentMsgId;

        streamResponse(
          sessionId,
          `Please analyze and clean the CSV file at: ${upload.filePath}`,
          agentMsgId,
        );
      } catch (error: any) {
        setUploadProgress(null);
        const errorMsgId = nextId();
        setMessages((prev) => [
          ...prev,
          {
            id: errorMsgId,
            role: 'agent',
            text: `**Error:** ${error.message}`,
            toolCalls: [],
            isStreaming: false,
            timestamp: Date.now(),
          },
        ]);
        setIsAgentRunning(false);
      }
    },
    [isAgentRunning, streamResponse],
  );

  const handleSendMessage = useCallback(
    (text: string) => {
      if (isAgentRunning || !session) return;
      setIsAgentRunning(true);

      // Add user message
      const userMsgId = nextId();
      setMessages((prev) => [
        ...prev,
        {
          id: userMsgId,
          role: 'user',
          text,
          toolCalls: [],
          isStreaming: false,
          timestamp: Date.now(),
        },
      ]);

      // Add empty agent message
      const agentMsgId = nextId();
      setMessages((prev) => [
        ...prev,
        {
          id: agentMsgId,
          role: 'agent',
          text: '',
          toolCalls: [],
          isStreaming: true,
          timestamp: Date.now(),
        },
      ]);
      agentMessageIdRef.current = agentMsgId;

      streamResponse(session.id, text, agentMsgId);
    },
    [isAgentRunning, session, streamResponse],
  );

  const handleSampleLoad = useCallback(() => {
    const file = createSampleFile();
    handleFileUpload(file);
  }, [handleFileUpload]);

  const handleReset = useCallback(() => {
    setMessages([]);
    setSession(null);
    setIsAgentRunning(false);
    setCanvas(null);
    agentMessageIdRef.current = null;
    messageIdCounter = 0;
  }, []);

  const isCanvasOpen = !!canvas;

  // Resize handle state
  const [chatWidthPct, setChatWidthPct] = useState(50);
  const isDragging = useRef(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDragging.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  }, []);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const pct = ((e.clientX - rect.left) / rect.width) * 100;
      // Clamp between 25% and 75%
      setChatWidthPct(Math.min(75, Math.max(25, pct)));
    };

    const handleMouseUp = () => {
      if (isDragging.current) {
        isDragging.current = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      }
    };

    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, []);

  return (
    <div className="h-screen flex flex-col bg-slate-50/50">
      <Header onReset={messages.length > 0 ? handleReset : undefined} />

      <div ref={containerRef} className="flex-1 flex min-h-0">
        {/* Chat pane */}
        <div
          className="flex flex-col min-h-0 transition-all duration-300 ease-in-out"
          style={{ width: isCanvasOpen ? `${chatWidthPct}%` : '100%' }}
        >
          <ChatContainer
            messages={messages}
            onSendMessage={handleSendMessage}
            onFileUpload={handleFileUpload}
            onSampleLoad={handleSampleLoad}
            isAgentRunning={isAgentRunning}
            hasSession={!!session}
            onOpenCanvas={openCanvas}
            uploadProgress={uploadProgress}
          />
        </div>

        {/* Resize handle */}
        {isCanvasOpen && (
          <div
            onMouseDown={handleMouseDown}
            className="w-1 flex-shrink-0 cursor-col-resize bg-slate-200 hover:bg-primary-400 active:bg-primary-500 transition-colors"
          />
        )}

        {/* Canvas pane */}
        <div
          className="overflow-hidden transition-all duration-300 ease-in-out"
          style={{
            width: isCanvasOpen ? `calc(${100 - chatWidthPct}% - 4px)` : '0',
            opacity: isCanvasOpen ? 1 : 0,
          }}
        >
          {canvas && (
            <Canvas
              messages={messages}
              onClose={closeCanvas}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
