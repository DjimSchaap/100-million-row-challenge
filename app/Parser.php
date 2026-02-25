<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numWorkers = 2;

        if ($fileSize < 1_000_000 || !function_exists('pcntl_fork')) {
            $this->parseSingleThread($inputPath, $outputPath);
            return;
        }

        $chunkSize = intdiv($fileSize, $numWorkers);
        $tmpDir = sys_get_temp_dir();
        $pids = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $startOffset = $w * $chunkSize;
            $endOffset = ($w === $numWorkers - 1) ? $fileSize : ($w + 1) * $chunkSize;

            $pid = pcntl_fork();
            if ($pid === 0) {
                $data = $this->processSegment($inputPath, $startOffset, $endOffset);
                $serialized = function_exists('igbinary_serialize')
                    ? igbinary_serialize($data)
                    : serialize($data);
                file_put_contents("{$tmpDir}/100m_worker_{$w}.dat", $serialized);
                exit(0);
            }
            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $tmpFile = "{$tmpDir}/100m_worker_{$w}.dat";
            $raw = file_get_contents($tmpFile);
            unlink($tmpFile);
            $data = function_exists('igbinary_unserialize')
                ? igbinary_unserialize($raw)
                : unserialize($raw);
            foreach ($data as $path => $dates) {
                if (isset($merged[$path])) {
                    foreach ($dates as $date => $count) {
                        if (isset($merged[$path][$date])) {
                            $merged[$path][$date] += $count;
                        } else {
                            $merged[$path][$date] = $count;
                        }
                    }
                } else {
                    $merged[$path] = $dates;
                }
            }
        }

        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT));
    }

    private function processSegment(string $inputPath, int $startOffset, int $endOffset): array
    {
        $data = [];
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 4 * 1024 * 1024);

        if ($startOffset > 0) {
            fseek($handle, $startOffset);
            fgets($handle);
        }

        $segmentSize = $endOffset - ftell($handle);
        $bytesRead = 0;

        while ($bytesRead < $segmentSize && ($line = fgets($handle)) !== false) {
            $len = strlen($line);
            $bytesRead += $len;
            $commaPos = $len - 27;
            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);
            if (isset($data[$path][$date])) {
                $data[$path][$date]++;
            } else {
                $data[$path][$date] = 1;
            }
        }

        fclose($handle);
        return $data;
    }

    private function parseSingleThread(string $inputPath, string $outputPath): void
    {
        $data = [];
        $handle = fopen($inputPath, 'r');

        while (($line = fgets($handle)) !== false) {
            $len = strlen($line);
            $commaPos = $len - 27;
            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);
            if (isset($data[$path][$date])) {
                $data[$path][$date]++;
            } else {
                $data[$path][$date] = 1;
            }
        }

        fclose($handle);

        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}