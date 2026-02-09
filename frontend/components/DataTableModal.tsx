import React, { useEffect, useState, useCallback, useRef } from 'react';
import { CleanedDataRow, ChatMessage } from '../types';
import { DataTable } from './DataTable';
import { ChatBubble } from './ChatBubble';
import { X, Send, MessageSquare, RefreshCw } from 'lucide-react';

interface DataTableModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: CleanedDataRow[] | null;
  totalRows: number;
  loading: boolean;
  onSendMessage: (text: string) => void;
  isAgentRunning: boolean;
  messages: ChatMessage[];
  onRefreshData: () => void;
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
  onRefreshData,
}) => {
  const [inputText, setInputText] = useState('');
  const [showChat, setShowChat] = useState(true);
  const inputRef = useRef<HTMLInputElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const chatContainerRef = useRef<HTMLDivElement>(null);
  const prevAgentRunning = useRef(isAgentRunning);

  // Auto-scroll chat to bottom when messages update
  useEffect(() => {
    if (showChat && chatEndRef.current) {
      chatEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, messages[messages.length - 1]?.text, showChat]);

  // Scroll to bottom when modal opens
  useEffect(() => {
    if (isOpen && showChat && chatEndRef.current) {
      // Use setTimeout to ensure DOM is ready
      setTimeout(() => {
        chatEndRef.current?.scrollIntoView({ behavior: 'auto' });
      }, 100);
    }
  }, [isOpen, showChat]);

  // Auto-refresh data when agent finishes
  useEffect(() => {
    if (prevAgentRunning.current && !isAgentRunning) {
      // Agent just finished - refresh the data
      onRefreshData();
    }
    prevAgentRunning.current = isAgentRunning;
  }, [isAgentRunning, onRefreshData]);

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
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold text-slate-800">Data Preview</h2>
            <button
              onClick={onRefreshData}
              disabled={loading}
              className="p-1.5 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors disabled:opacity-50"
              aria-label="Refresh data"
              title="Refresh data"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </div>
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

          {/* Chat Panel - exact replica of main chat */}
          {showChat && (
            <div className="w-[420px] flex-shrink-0 flex flex-col bg-slate-50/50">
              {/* Messages - using ChatBubble like main chat */}
              <div ref={chatContainerRef} className="flex-1 overflow-y-auto px-3 py-4">
                <div className="space-y-1 [&>div]:scale-90 [&>div]:origin-top-left [&>div]:w-[111%]">
                  {messages.map((msg) => (
                    <ChatBubble
                      key={msg.id}
                      message={msg}
                      onSendMessage={onSendMessage}
                    />
                  ))}
                </div>
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
