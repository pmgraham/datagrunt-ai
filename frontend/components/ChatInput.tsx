import React, { useState, useRef, useCallback } from 'react';
import { Send, Paperclip, FileSpreadsheet } from 'lucide-react';

const SAMPLE_CSV = `ID,Full Name,Email Address,Phone,Join_Date,Department,Salary
1,John Doe,john.doe@example.com,555-123-4567,2023-01-15,Engineering,90000
2,JANE SMITH,jane.smith@test.co,(555) 987-6543,Jan 16 2023,Marketing,
3,Bob Johnson,,555.111.2222,2022/12/01,Sales,65000
4, Alice  Williams ,alice.w@domain.net,555 222 3333,03-15-2023,Engineering,92000
5,,invalid_row,,,
6,Charlie Brown,charlie.brown@email.com,,2023-04-01,Support,45000
7,David Jones,david.jones@example.org,555-444-3333,2023-05-20,Marketing,70000
8,Eve,eve@security.com,555-666-7777,2023-06-10,Security,120000
9,Frank Miller,frank.m@design.io,555-333-2222,2023-02-28,Design,85000
10,Grace Hopper,grace@navy.mil,555-000-1111,1980-12-09,Engineering,150000`;

interface ChatInputProps {
  onSendMessage: (text: string) => void;
  onFileUpload: (file: File) => void;
  onSampleLoad: () => void;
  disabled: boolean;
  hasSession: boolean;
}

function createSampleFile(): File {
  const blob = new Blob([SAMPLE_CSV], { type: 'text/csv' });
  return new File([blob], 'sample_employees.csv', { type: 'text/csv' });
}

export { createSampleFile, SAMPLE_CSV };

export const ChatInput: React.FC<ChatInputProps> = ({
  onSendMessage,
  onFileUpload,
  onSampleLoad,
  disabled,
  hasSession,
}) => {
  const [text, setText] = useState('');
  const [isDragging, setIsDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const validateAndUpload = useCallback((file: File) => {
    setError(null);
    if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
      setError('Please upload a valid CSV file.');
      return;
    }
    onFileUpload(file);
  }, [onFileUpload]);

  const handleSubmit = useCallback(() => {
    const trimmed = text.trim();
    if (!trimmed || disabled) return;
    onSendMessage(trimmed);
    setText('');
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
  }, [text, disabled, onSendMessage]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  }, [handleSubmit]);

  const handleTextareaChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setText(e.target.value);
    // Auto-resize
    const el = e.target;
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 160) + 'px';
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files?.[0]) {
      validateAndUpload(e.dataTransfer.files[0]);
    }
  }, [validateAndUpload]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) {
      validateAndUpload(e.target.files[0]);
      e.target.value = '';
    }
  }, [validateAndUpload]);

  return (
    <div className="border-t border-slate-200 bg-white px-4 py-3">
      {error && (
        <div className="mb-2 p-2 bg-red-50 text-red-600 rounded-lg text-xs">
          {error}
        </div>
      )}

      <div
        className={`flex items-end gap-2 rounded-xl border transition-colors ${
          isDragging
            ? 'border-primary-400 bg-primary-50/50'
            : 'border-slate-200 bg-slate-50'
        } px-3 py-2`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        {!hasSession && (
          <>
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              disabled={disabled}
              className="flex-shrink-0 p-1.5 text-slate-400 hover:text-slate-600 disabled:opacity-50 transition-colors"
              title="Upload CSV"
            >
              <Paperclip className="w-5 h-5" />
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileInputChange}
              className="hidden"
            />
          </>
        )}

        <textarea
          ref={textareaRef}
          value={text}
          onChange={handleTextareaChange}
          onKeyDown={handleKeyDown}
          placeholder={hasSession ? 'Type a message...' : 'Upload a CSV to get started, or type a message...'}
          disabled={disabled}
          rows={1}
          className="flex-1 resize-none bg-transparent text-sm text-slate-900 placeholder-slate-400 focus:outline-none disabled:opacity-50 py-1.5 max-h-40"
        />

        <button
          type="button"
          onClick={handleSubmit}
          disabled={disabled || !text.trim()}
          className="flex-shrink-0 p-1.5 text-primary-600 hover:text-primary-700 disabled:text-slate-300 disabled:hover:text-slate-300 transition-colors"
          title="Send message"
        >
          <Send className="w-5 h-5" />
        </button>
      </div>

      {!hasSession && (
        <div className="mt-2 flex justify-center">
          <button
            type="button"
            onClick={onSampleLoad}
            disabled={disabled}
            className="flex items-center text-xs text-primary-600 hover:text-primary-700 font-medium hover:underline disabled:opacity-50 transition-colors"
          >
            <FileSpreadsheet className="w-3.5 h-3.5 mr-1" />
            Try with sample data
          </button>
        </div>
      )}
    </div>
  );
};
