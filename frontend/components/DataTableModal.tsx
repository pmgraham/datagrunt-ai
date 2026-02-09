import React, { useEffect, useState, useCallback, useRef } from 'react';
import { CleanedDataRow } from '../types';
import { DataTable } from './DataTable';
import { X, Send } from 'lucide-react';

interface DataTableModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: CleanedDataRow[] | null;
  totalRows: number;
  loading: boolean;
  onSendMessage: (text: string) => void;
  isAgentRunning: boolean;
}

export const DataTableModal: React.FC<DataTableModalProps> = ({
  isOpen,
  onClose,
  data,
  totalRows,
  loading,
  onSendMessage,
  isAgentRunning,
}) => {
  const [inputText, setInputText] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

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
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
            aria-label="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content - scrollable area for both horizontal and vertical */}
        <div className="flex-1 min-h-0 p-6">
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

        {/* Chat Input */}
        <div className="border-t border-slate-200 bg-slate-50 px-6 py-4">
          <div className="flex items-center gap-3 max-w-3xl mx-auto">
            <input
              ref={inputRef}
              type="text"
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question about this data..."
              disabled={isAgentRunning}
              className="flex-1 px-4 py-2.5 text-sm rounded-lg border border-slate-200 bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent disabled:opacity-50"
            />
            <button
              type="button"
              onClick={handleSubmit}
              disabled={isAgentRunning || !inputText.trim()}
              className="p-2.5 rounded-lg bg-primary-600 text-white hover:bg-primary-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
              title="Send message"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
          <p className="text-xs text-slate-400 text-center mt-2">
            Messages appear in the main chat
          </p>
        </div>
      </div>
    </div>
  );
};
