<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;

// This is a completely AI-generated class, please ignore.
final class BenchmarkHistoryCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): void
    {
        $historyFile = __DIR__ . '/../../leaderboard-history.csv';
        $leaderboardRelativePath = 'leaderboard.csv';

        // Get all commits that modified leaderboard.csv since 2026-03-01
        $gitLog = shell_exec("git log --format='%H|%ci' --since='2026-03-01' --follow -- {$leaderboardRelativePath}");

        if (empty($gitLog)) {
            echo "No git history found for leaderboard.csv\n";
            return;
        }

        $commits = array_filter(explode("\n", trim($gitLog)));
        $commits = array_reverse($commits); // Process oldest first

        $historyData = [];

        foreach ($commits as $commitLine) {
            $this->info("Processing commit: {$commitLine}");
            [$commitHash, $commitDate] = explode('|', $commitLine, 2);

            // Get the diff for this commit to see what was added/changed
            $diff = shell_exec("git show {$commitHash} -- {$leaderboardRelativePath} 2>/dev/null");

            if (empty($diff)) {
                continue;
            }

            $lines = explode("\n", $diff);

            foreach ($lines as $line) {
                // Only process lines that were added (start with +)
                if (!str_starts_with($line, '+') || str_starts_with($line, '+++')) {
                    continue;
                }

                // Remove the leading +
                $line = substr($line, 1);

                if (empty($line) || str_starts_with($line, 'entry_date,')) {
                    continue;
                }

                // Parse CSV line
                $parts = explode(',', str_replace(';', ',', $line));

                if (count($parts) >= 3) {
                    $historyData[] = [
                        'entry_date' => date('Y-m-d H:i:s', strtotime($commitDate)),
                        'branch_name' => $parts[1],
                        'time' => $parts[2],
                    ];
                }
            }
        }

        // Sort by date first
        usort($historyData, fn($a, $b) => strcmp($a['entry_date'], $b['entry_date']));

        // Track the current best time for each branch
        $currentBestByBranch = [];

        // Get all unique timestamps
        $timestamps = array_unique(array_column($historyData, 'entry_date'));

        // Build normalized data: for each timestamp, include all branches with their current best time
        $normalizedData = [];

        foreach ($historyData as $entry) {
            $branch = $entry['branch_name'];
            $time = (float)$entry['time'];

            // Update the current best for this branch
            if (!isset($currentBestByBranch[$branch]) || $time < $currentBestByBranch[$branch]) {
                $currentBestByBranch[$branch] = $time;
            }

            // Add a snapshot of all branches at this timestamp
            foreach ($currentBestByBranch as $branchName => $bestTime) {
                $normalizedData[] = [
                    'entry_date' => $entry['entry_date'],
                    'branch_name' => $branchName,
                    'time' => $bestTime,
                ];
            }
        }

        // Exclude certain branches
        $excludedBranches = ['brendt', 'ghostwriter'];
        $currentBestByBranch = array_diff_key($currentBestByBranch, array_flip($excludedBranches));

        // Get top 10 branches by best time
        asort($currentBestByBranch);
        $topBranches = array_slice(array_keys($currentBestByBranch), 0, 4);
        sort($topBranches);

        // Group by timestamp, with each branch as a column
        $pivotedData = [];

        foreach ($normalizedData as $entry) {
            if (!in_array($entry['branch_name'], $topBranches)) {
                continue;
            }

            $date = $entry['entry_date'];
            if (!isset($pivotedData[$date])) {
                $pivotedData[$date] = [];
            }
            $pivotedData[$date][$entry['branch_name']] = $entry['time'];
        }

        // Write history to file
        $fp = fopen($historyFile, 'w');

        // Header: entry_date, branch1, branch2, ...
        fputs($fp, 'entry_date,' . implode(',', $topBranches) . "\n");

        foreach ($pivotedData as $date => $branches) {
            $row = [$date];
            foreach ($topBranches as $branch) {
                $row[] = $branches[$branch] ?? '';
            }
            fputs($fp, implode(',', $row) . "\n");
        }

        fclose($fp);

        $this->success('Done.');
    }
}