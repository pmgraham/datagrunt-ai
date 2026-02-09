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
 * Removes duplicate content if the model accidentally output something twice.
 * Handles both report headers and general repeated content.
 */
export function deduplicateReport(text: string): string {
  // First check for repeated "Executive Summary" headers
  const execSummaryPattern = /#{2,3}\s*Executive Summary/gi;
  const matches = [...text.matchAll(execSummaryPattern)];

  if (matches.length > 1) {
    // Truncate at the start of the second occurrence
    const secondMatchIdx = matches[1].index!;
    return text.substring(0, secondMatchIdx).trim();
  }

  // Check if the second half is a duplicate of the first half
  // (handles cases like "Done! ... Done! ...")
  const trimmed = text.trim();
  const len = trimmed.length;
  if (len > 20) {
    // Try to find a repeated block
    for (let splitPoint = Math.floor(len * 0.4); splitPoint <= Math.floor(len * 0.6); splitPoint++) {
      const firstHalf = trimmed.substring(0, splitPoint).trim();
      const secondHalf = trimmed.substring(splitPoint).trim();
      // Check if second half starts with first half (allowing for slight variations)
      if (secondHalf.startsWith(firstHalf.substring(0, Math.min(50, firstHalf.length)))) {
        return firstHalf;
      }
    }
  }

  return text;
}
