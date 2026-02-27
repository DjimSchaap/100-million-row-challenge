<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;
use const WNOHANG;

use function chr;
use function count;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function feof;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function ord;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;

final class Parser
{
    protected const int WORKERS = 10;

    protected const int READ_CHUNK = 131072;

    protected const int DISCOVER_SIZE = 2097152;

    protected array $dateIds = [];

    protected array $dates = [];

    protected array $datePrefixes = [];

    protected int $dateCount = 0;

    protected array $pathIds = [];

    protected array $paths = [];

    protected int $pathCount = 0;

    protected int $position = 0;

    protected int $fileSize = 0;

    protected array $children = [];

    protected array $boundaries = [0];

    protected array $escapedPaths = [];

    protected bool $firstPath = true;

    protected bool $firstDate = true;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $this->fileSize = filesize($inputPath);

        for ($year = 20; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDays = match ($month) {
                    2 => (($year + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $monthString = ($month < 10 ? '0' : '') . $month;
                $yearMonthString = $year . '-' . $monthString . '-';
                for ($day = 1; $day <= $maxDays; $day++) {
                    $key = $yearMonthString . (($day < 10 ? '0' : '') . $day);
                    $this->dateIds[$key] = $this->dateCount;
                    $this->dates[$this->dateCount] = $key;
                    $this->dateCount++;
                }
            }
        }

        $binaryResource = fopen($inputPath, 'rb');
        stream_set_read_buffer($binaryResource, 0);
        $warmUpSize = $this->fileSize > static::DISCOVER_SIZE ? static::DISCOVER_SIZE : $this->fileSize;
        $raw = fread($binaryResource, $warmUpSize);

        for ($worker = 1; $worker < static::WORKERS; $worker++) {
            fseek($binaryResource, (int) ($this->fileSize * $worker / static::WORKERS));
            fgets($binaryResource);
            $this->boundaries[] = ftell($binaryResource);
        }

        fclose($binaryResource);

        $lastNewLine = strrpos($raw, "\n");
        if ($lastNewLine === false) {
            $lastNewLine = 0;
        }

        while ($this->position < $lastNewLine) {
            $newLinePosition = strpos($raw, "\n", $this->position + 52);
            if ($newLinePosition === false) {
                break;
            }

            $slug = substr($raw, $this->position + 25, $newLinePosition - $this->position - 51);
            if (! isset($this->pathIds[$slug])) {
                $this->pathIds[$slug] = $this->pathCount;
                $this->paths[$this->pathCount] = $slug;
                $this->pathCount++;
            }

            $this->position = $newLinePosition + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (! isset($this->pathIds[$slug])) {
                $this->pathIds[$slug] = $this->pathCount;
                $this->paths[$this->pathCount] = $slug;
                $this->pathCount++;
            }
        }

        $this->boundaries[] = $this->fileSize;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();

        for ($worker = 0; $worker < static::WORKERS - 1; $worker++) {
            $tmpFile = "{$tmpDir}/workers-{$myPid}-{$worker}";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->parseRangeToSparseFile($inputPath, $this->boundaries[$worker], $this->boundaries[$worker + 1], $tmpFile);
                exit(0);
            }
            $this->children[$pid] = $tmpFile;
        }

        $counts = $this->parseRangeToCounts($inputPath, $this->boundaries[static::WORKERS - 1], $this->boundaries[static::WORKERS]);

        $pending = count($this->children);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }

            if (! isset($this->children[$pid])) {
                continue;
            }

            $tmpFile = $this->children[$pid];
            $this->mergeSparseFileIntoCounts($tmpFile, $counts);
            unlink($tmpFile);
            $pending--;
        }

        $this->writeJson($outputPath, $counts);
    }

    protected function parseRangeToCounts(string $inputPath, int $start, int $end): array
    {
        $counts = [];
        $total = $this->pathCount * $this->dateCount;
        for ($i = 0; $i < $total; $i++) {
            $counts[$i] = 0;
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > static::READ_CHUNK ? static::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) {
                break;
            }

            $remaining -= $chunkLen;

            $lastNewLine = strrpos($chunk, "\n");
            if ($lastNewLine === false) {
                break;
            }

            $tail = $chunkLen - $lastNewLine - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $position = 25;

            while ($position < $lastNewLine) {
                $separatorPosition = strpos($chunk, ',', $position);
                if ($separatorPosition === false || $separatorPosition >= $lastNewLine) {
                    break;
                }

                $pathId = $this->pathIds[substr($chunk, $position, $separatorPosition - $position)];
                $dateId = $this->dateIds[substr($chunk, $separatorPosition + 3, 8)];
                $counts[$pathId * $this->dateCount + $dateId]++;

                $position = $separatorPosition + 52;
            }
        }

        fclose($handle);

        return $counts;
    }

    protected function parseRangeToSparseFile(string $inputPath, int $start, int $end, string $outputPath): void
    {
        $counts = $this->parseRangeToCounts($inputPath, $start, $end);

        $output = fopen($outputPath, 'wb');
        stream_set_write_buffer($output, 1048576);

        $total = $this->pathCount * $this->dateCount;
        for ($cellIndex = 0; $cellIndex < $total; $cellIndex++) {
            $count = $counts[$cellIndex];
            if ($count === 0) {
                continue;
            }

            fwrite(
                $output,
                chr($cellIndex & 0xFF)
                . chr(($cellIndex >> 8) & 0xFF)
                . chr(($cellIndex >> 16) & 0xFF)
                . chr(($cellIndex >> 24) & 0xFF)
                . chr($count & 0xFF)
                . chr(($count >> 8) & 0xFF)
            );
        }

        fclose($output);
    }

    protected function mergeSparseFileIntoCounts(string $filePath, array &$counts): void
    {
        $handle = fopen($filePath, 'rb');
        stream_set_read_buffer($handle, 0);

        $carry = '';

        while (! feof($handle)) {
            $chunk = fread($handle, 1048576);
            if ($chunk === '' || $chunk === false) {
                break;
            }

            if ($carry !== '') {
                $chunk = $carry . $chunk;
                $carry = '';
            }

            $len = strlen($chunk);
            $usable = $len - ($len % 6);

            if ($usable !== $len) {
                $carry = substr($chunk, $usable);
                $chunk = substr($chunk, 0, $usable);
                $len = $usable;
            }

            for ($i = 0; $i < $len; $i += 6) {
                $cellIndex =
                    (ord($chunk[$i])
                    | (ord($chunk[$i + 1]) << 8)
                    | (ord($chunk[$i + 2]) << 16)
                    | (ord($chunk[$i + 3]) << 24));

                $count =
                    (ord($chunk[$i + 4])
                    | (ord($chunk[$i + 5]) << 8));

                $counts[$cellIndex] += $count;
            }
        }

        fclose($handle);
    }

    protected function writeJson(string $outputPath, array $counts): void
    {
        $writeBinary = fopen($outputPath, 'wb');
        stream_set_write_buffer($writeBinary, 1048576);
        fwrite($writeBinary, '{');

        $this->datePrefixes = [];
        for ($dateIndex = 0; $dateIndex < $this->dateCount; $dateIndex++) {
            $this->datePrefixes[$dateIndex] = "        \"20{$this->dates[$dateIndex]}\": ";
        }

        $this->pathCount = count($this->paths);
        for ($pathIndex = 0; $pathIndex < $this->pathCount; $pathIndex++) {
            $this->escapedPaths[$pathIndex] = "\"\\/blog\\/" . str_replace('/', '\\/', $this->paths[$pathIndex]) . "\"";
        }

        for ($pathIndex = 0; $pathIndex < $this->pathCount; $pathIndex++) {
            $base = $pathIndex * $this->dateCount;

            $hasData = false;
            for ($dateIndex = 0; $dateIndex < $this->dateCount; $dateIndex++) {
                if ($counts[$base + $dateIndex] > 0) {
                    $hasData = true;
                    break;
                }
            }

            if (! $hasData) {
                continue;
            }

            $buffer = $this->firstPath ? "\n    " : ",\n    ";
            $this->firstPath = false;
            $buffer .= $this->escapedPaths[$pathIndex] . ": {\n";

            $this->firstDate = true;
            for ($dateIndex = 0; $dateIndex < $this->dateCount; $dateIndex++) {
                $count = $counts[$base + $dateIndex];
                if ($count === 0) {
                    continue;
                }

                $buffer .= $this->firstDate ? '' : ",\n";
                $this->firstDate = false;
                $buffer .= $this->datePrefixes[$dateIndex] . $count;
            }

            $buffer .= "\n    }";
            fwrite($writeBinary, $buffer);
        }

        fwrite($writeBinary, "\n}");
        fclose($writeBinary);
    }
}
