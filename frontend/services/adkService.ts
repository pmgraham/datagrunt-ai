import { CleanedDataRow } from '../types';

const API_BASE = '/api';
const APP_NAME = 'clean_csv_agent';
const USER_ID = 'web_user';

export const FRIENDLY_NAMES: Record<string, string> = {
  load_csv: 'Loading CSV into DuckDB',
  inspect_raw_file: 'Inspecting raw file',
  normalize_column_names: 'Normalizing column names',
  detect_column_overflow: 'Detecting column overflow',
  repair_column_overflow: 'Repairing column overflow',
  detect_era_in_years: 'Detecting era in years',
  extract_era_column: 'Extracting era column',
  // Batch tools (optimized)
  profile_all_columns: 'Profiling all columns',
  audit_all_columns: 'Auditing all columns',
  analyze_all_patterns: 'Analyzing all patterns',
  // Legacy per-column tools
  get_smart_schema: 'Analyzing schema',
  suggest_type_coercion: 'Checking column types',
  detect_type_pollution: 'Detecting type issues',
  detect_advanced_anomalies: 'Scanning for outliers',
  detect_date_formats: 'Checking date formats',
  get_value_distribution: 'Analyzing value distributions',
  check_column_logic: 'Checking column logic',
  query_data: 'Querying data',
  preview_full_plan: 'Previewing cleaning plan',
  execute_cleaning_plan: 'Applying cleaning changes',
  validate_cleaned_data: 'Validating cleaned data',
  Profiler: 'Running Profiler agent',
  Auditor: 'Running Auditor agent',
  PatternExpert: 'Running Pattern Expert agent',
};

interface UploadResult {
  filePath: string;
  rowCount: number;
}

export function uploadFile(
  file: File,
  onProgress?: (percent: number) => void,
): Promise<UploadResult> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const form = new FormData();
    form.append('file', file);

    if (onProgress) {
      xhr.upload.addEventListener('progress', (e) => {
        if (e.lengthComputable) {
          onProgress(Math.round((e.loaded / e.total) * 100));
        }
      });
    }

    xhr.addEventListener('load', () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        const data = JSON.parse(xhr.responseText);
        resolve({ filePath: data.file_path, rowCount: data.row_count });
      } else {
        reject(new Error(`Upload failed (${xhr.status}): ${xhr.responseText}`));
      }
    });

    xhr.addEventListener('error', () => reject(new Error('Network error during upload')));

    xhr.open('POST', `${API_BASE}/upload`);
    xhr.send(form);
  });
}

export async function createSession(): Promise<string> {
  const res = await fetch(
    `${API_BASE}/apps/${APP_NAME}/users/${USER_ID}/sessions`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' } },
  );
  if (!res.ok) throw new Error(`Failed to create session: ${res.status}`);
  const session = await res.json();
  return session.id;
}

export interface StreamCallbacks {
  onToken: (token: string) => void;
  onToolCall: (name: string, friendlyName: string) => void;
  onCleanedFile?: (filePath: string) => void;
  onComplete: (fullText: string) => void;
  onError: (error: Error) => void;
}

export async function streamAgentMessage(
  sessionId: string,
  message: string,
  callbacks: StreamCallbacks,
): Promise<void> {
  const body = {
    app_name: APP_NAME,
    user_id: USER_ID,
    session_id: sessionId,
    new_message: {
      role: 'user',
      parts: [{ text: message }],
    },
    streaming: true,
  };

  let res: Response;
  try {
    res = await fetch(`${API_BASE}/run_sse`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (err: any) {
    callbacks.onError(new Error(`Network error: ${err.message}`));
    return;
  }

  if (!res.ok) {
    const err = await res.text();
    callbacks.onError(new Error(`Agent run failed (${res.status}): ${err}`));
    return;
  }

  let fullText = '';
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const jsonStr = line.slice(6).trim();
        if (!jsonStr) continue;

        try {
          const event = JSON.parse(jsonStr);
          if (event.content?.parts) {
            for (const part of event.content.parts) {
              if (part.functionCall) {
                const name = part.functionCall.name;
                callbacks.onToolCall(name, FRIENDLY_NAMES[name] || `Running ${name}`);
              }
              if (callbacks.onCleanedFile) {
                const cleanedFile = part.functionResponse?.response?.cleaned_file;
                if (cleanedFile) {
                  callbacks.onCleanedFile(cleanedFile);
                }
              }
              if (part.text) {
                fullText += part.text;
                callbacks.onToken(part.text);
              }
            }
          }
        } catch {
          // skip malformed events
        }
      }
    }
  } catch (err: any) {
    callbacks.onError(new Error(`Stream error: ${err.message}`));
    return;
  }

  callbacks.onComplete(fullText);
}

export function extractCleanedFilePath(text: string): string | null {
  // Match various phrasings the agent might use:
  // "saved at: /path", "file is at: /path", "saved to /path", or backtick-wrapped path
  const match = text.match(/(?:saved|file is|data is)[:\s]+`?([^\s`]+_cleaned\.csv)`?/i)
    || text.match(/at[:\s]+`?([^\s`]+_cleaned\.csv)`?/i)
    || text.match(/`([^\s`]+_cleaned\.csv)`/);
  return match ? match[1] : null;
}

export interface PreviewResult {
  rows: CleanedDataRow[];
  total: number;
}

export async function fetchPreviewRows(
  table: string = 'data',
  limit: number = 100,
  offset: number = 0,
): Promise<PreviewResult> {
  const params = new URLSearchParams({
    table,
    limit: String(limit),
    offset: String(offset),
  });
  const res = await fetch(`${API_BASE}/preview?${params}`);
  if (!res.ok) return { rows: [], total: 0 };

  const data = await res.json();
  return { rows: data.rows, total: data.total };
}

export function getDownloadUrl(filePath: string): string {
  return `${API_BASE}/download?file_path=${encodeURIComponent(filePath)}&download=true`;
}
