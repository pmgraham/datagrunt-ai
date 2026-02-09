import React, { useEffect, useState, useCallback, useRef } from 'react';
import { CleanedDataRow, ChatMessage } from '../types';
import { DataTable } from './DataTable';
import { MarkdownReport } from './MarkdownReport';
import { X, Send, MessageSquare } from 'lucide-react';

interface DataTableModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: CleanedDataRow[] | null;
  totalRows: number;
  loading: boolean;
  onSendMessage: (text: string) => void;
  isAgentRunning: boolean;
  messages: ChatMessage[];
}

export const DataTableModal: React.FC<DataTableModalProps> = ({
  isOpen,
  onClose,
  data,
  totalRows,
  loading,
  onSendMessage,
  isAgentRunning,
  messages,
}) => {
  const [inputText, setInputText] = useState('');
  const [showChat, setShowChat] = useState(true);
  const inputRef = useRef<HTMLInputElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll chat to bottom when new messages arrive
  useEffect(() => {
    if (showChat && chatEndRef.current) {
      chatEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, showChat]);

  const handleSubmit = useCallback(() => {
    const trimmed = inputText.trim();
    if (!trimmed || isAgentRunning) return;
    onSendMessage(trimmed);
    setInputText('');
  }, [inputText, isAgentRunning, onSendMessage]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
    // Don't close modal on Escape if input is focused
    if (e.key === 'Escape') {
      e.stopPropagation();
      inputRef.current?.blur();
    }
  }, [handleSubmit]);

  // Close on Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      document.body.style.overflow = 'hidden';
    }
    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = '';
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative w-[95vw] h-[90vh] bg-white rounded-xl shadow-2xl flex flex-col overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200 bg-slate-50">
          <h2 className="text-lg font-semibold text-slate-800">Data Preview</h2>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowChat(!showChat)}
              className={`p-2 rounded-lg transition-colors ${
                showChat
                  ? 'text-primary-600 bg-primary-50 hover:bg-primary-100'
                  : 'text-slate-400 hover:text-slate-600 hover:bg-slate-200'
              }`}
              aria-label={showChat ? 'Hide chat' : 'Show chat'}
              title={showChat ? 'Hide chat' : 'Show chat'}
            >
              <MessageSquare className="w-5 h-5" />
            </button>
            <button
              onClick={onClose}
              className="p-2 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
              aria-label="Close"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Content - split view with data and chat */}
        <div className="flex-1 min-h-0 flex">
          {/* Data Table Panel */}
          <div className={`flex-1 min-w-0 p-6 overflow-auto ${showChat ? 'border-r border-slate-200' : ''}`}>
            {loading && (
              <div className="flex items-center justify-center py-12 text-slate-400">
                <div className="animate-spin w-5 h-5 border-2 border-slate-300 border-t-primary-600 rounded-full mr-3" />
                Loading data preview...
              </div>
            )}
            {!loading && data && (
              <DataTable data={data} totalRows={totalRows} fullHeight />
            )}
          </div>

          {/* Chat Panel */}
          {showChat && (
            <div className="w-96 flex-shrink-0 flex flex-col bg-slate-50">
              {/* Messages */}
              <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {messages.filter(m => m.text).map((msg) => (
                  <div
                    key={msg.id}
                    className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                  >
                    <div
                      className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
                        msg.role === 'user'
                          ? 'bg-primary-600 text-white'
                          : 'bg-white border border-slate-200 text-slate-700'
                      }`}
                    >
                      {msg.role === 'agent' ? (
                        <div className="text-sm [&_h1]:text-base [&_h2]:text-sm [&_h3]:text-sm [&_p]:mb-2 [&_p]:text-sm [&_table]:text-xs">
                          <MarkdownReport content={msg.text} bare />
                        </div>
                      ) : (
                        <p>{msg.text}</p>
                      )}
                      {msg.isStreaming && (
                        <span className="inline-block w-1.5 h-4 ml-1 bg-slate-400 animate-pulse rounded-sm" />
                      )}
                    </div>
                  </div>
                ))}
                <div ref={chatEndRef} />
              </div>

              {/* Input */}
              <div className="border-t border-slate-200 bg-white p-4">
                <div className="flex items-center gap-2">
                  <input
                    ref={inputRef}
                    type="text"
                    value={inputText}
                    onChange={(e) => setInputText(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask about this data..."
                    disabled={isAgentRunning}
                    className="flex-1 px-3 py-2 text-sm rounded-lg border border-slate-200 bg-slate-50 text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent disabled:opacity-50"
                  />
                  <button
                    type="button"
                    onClick={handleSubmit}
                    disabled={isAgentRunning || !inputText.trim()}
                    className="p-2 rounded-lg bg-primary-600 text-white hover:bg-primary-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
                    title="Send message"
                  >
                    <Send className="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
