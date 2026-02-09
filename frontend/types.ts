export interface CleanedDataRow {
  [key: string]: string | number | boolean | null;
}

export interface FileData {
  name: string;
  size: number;
  file: File;
}

export type MessageRole = 'user' | 'agent';

export interface ToolStatus {
  name: string;
  friendlyName: string;
}

export interface ChatMessage {
  id: string;
  role: MessageRole;
  text: string;
  toolCalls: ToolStatus[];
  fileAttachment?: { name: string; size: number; file: File };
  downloadableFile?: { path: string; name: string };
  isStreaming: boolean;
  timestamp: number;
}

export interface SessionState {
  id: string;
  filePath: string;
  fileName: string;
  rowCount: number;
}

export interface CanvasState {
  activeTab: 'report' | 'data';
}
