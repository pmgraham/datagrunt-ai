import React from 'react';
import { ChatMessage } from '../types';
import { WelcomeScreen } from './WelcomeScreen';
import { MessageList } from './MessageList';
import { ChatInput } from './ChatInput';

interface ChatContainerProps {
  messages: ChatMessage[];
  onSendMessage: (text: string) => void;
  onFileUpload: (file: File) => void;
  onSampleLoad: () => void;
  isAgentRunning: boolean;
  hasSession: boolean;
  onOpenCanvas?: () => void;
  uploadProgress?: number | null;
}

export const ChatContainer: React.FC<ChatContainerProps> = ({
  messages,
  onSendMessage,
  onFileUpload,
  onSampleLoad,
  isAgentRunning,
  hasSession,
  onOpenCanvas,
  uploadProgress,
}) => {
  if (messages.length === 0) {
    return (
      <div className="flex-1 flex flex-col">
        <WelcomeScreen onFileUpload={onFileUpload} onSampleLoad={onSampleLoad} />
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <MessageList
        messages={messages}
        onOpenCanvas={onOpenCanvas}
        uploadProgress={uploadProgress}
        onSendMessage={onSendMessage}
        isAgentRunning={isAgentRunning}
      />
      <ChatInput
        onSendMessage={onSendMessage}
        onFileUpload={onFileUpload}
        onSampleLoad={onSampleLoad}
        disabled={isAgentRunning}
        hasSession={hasSession}
      />
    </div>
  );
};
