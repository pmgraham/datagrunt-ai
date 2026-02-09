/**
 * Heuristic to distinguish detailed reports from short conversational replies.
 * A message is considered a "report" if it's long or contains markdown section headers.
 */
export function isReportMessage(text: string): boolean {
  if (text.length > 500) return true;
  if (/^#{1,3}\s/m.test(text)) return true;
  return false;
}
