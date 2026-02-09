/**
 * Heuristic to distinguish detailed reports from short conversational replies.
 * A message is considered a "report" if it's long or contains markdown section headers.
 */
export function isReportMessage(text: string): boolean {
  if (text.length > 500) return true;
  if (/^#{1,3}\s/m.test(text)) return true;
  return false;
}

/**
 * Removes duplicate report content if the model accidentally output the report twice.
 * Looks for repeated "Executive Summary" headers and truncates at the second occurrence.
 */
export function deduplicateReport(text: string): string {
  const execSummaryPattern = /^#{2,3}\s*Executive Summary/im;
  const matches = text.match(new RegExp(execSummaryPattern.source, 'gim'));

  if (matches && matches.length > 1) {
    const firstIdx = text.search(execSummaryPattern);
    const afterFirst = firstIdx + matches[0].length;
    const rest = text.substring(afterFirst);
    const secondInRest = rest.search(execSummaryPattern);

    if (secondInRest >= 0) {
      return text.substring(0, afterFirst + secondInRest).trim();
    }
  }

  return text;
}
