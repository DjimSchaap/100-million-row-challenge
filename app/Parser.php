<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;
use const WNOHANG;

use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
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

    protected array $dateIdChars = [];

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

    protected array $yearOffsets = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $this->fileSize = filesize($inputPath);

        $this->yearOffsets = [];

        $offset = 0;
        for ($year = 20; $year <= 26; $year++) {
            $this->yearOffsets[$year] = $offset;

            for ($month = 1; $month <= 12; $month++) {
                $maxDays = match ($month) {
                    2 => (($year + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $offset += $maxDays;
            }
        }

        for ($year = 20; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDays = match ($month) {
                    2 => (($year + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $monthString = ($month < 10 ? '0' : '') . $month;
                $yearMonthString = $year . '-' . $monthString . '-';
                for ($days = 1; $days <= $maxDays; $days++) {
                    $key = $yearMonthString . (($days < 10 ? '0' : '') . $days);
                    $this->dateIds[$key] = $this->dateCount;
                    $this->dates[$this->dateCount] = $key;
                    $this->dateCount++;
                }
            }
        }

        foreach ($this->dateIds as $date => $id) {
            $this->dateIdChars[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $binaryResource = fopen($inputPath, 'rb');
        stream_set_read_buffer($binaryResource, 0);
        $warmUpSize = $this->fileSize > static::DISCOVER_SIZE ? static::DISCOVER_SIZE : $this->fileSize;
        $raw = fread($binaryResource, $warmUpSize);

        for ($workers = 1; $workers < static::WORKERS; $workers++) {
            fseek($binaryResource, (int) ($this->fileSize * $workers / static::WORKERS));
            fgets($binaryResource);
            $this->boundaries[] = ftell($binaryResource);
        }

        fclose($binaryResource);

        $lastNewLine = strrpos($raw, "\n");

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

        $totalCells = $this->pathCount * $this->dateCount;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();

        for ($worker = 0; $worker < static::WORKERS - 1; $worker++) {
            $tmpFile = "{$tmpDir}/workers-{$myPid}-{$worker}";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange($inputPath, $this->boundaries[$worker], $this->boundaries[$worker + 1]);

                $fh = fopen($tmpFile, 'wb');
                stream_set_write_buffer($fh, 1_048_576);

                $writeChunk = 32_768;
                $binary = '';
                $binaryLen = 0;

                for ($i = 0; $i < $totalCells; $i++) {
                    $v = $wCounts[$i];
                    $binary .= chr($v & 0xFF) . chr(($v >> 8) & 0xFF);
                    $binaryLen += 2;

                    if ($binaryLen >= $writeChunk * 2) {
                        fwrite($fh, $binary);
                        $binary = '';
                        $binaryLen = 0;
                    }
                }

                if ($binaryLen !== 0) {
                    fwrite($fh, $binary);
                }

                fclose($fh);
                exit(0);
            }
            $this->children[$pid] = $tmpFile;
        }

        $counts = $this->parseRange($inputPath, $this->boundaries[static::WORKERS - 1], $this->boundaries[static::WORKERS]);

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

            $fh = fopen($tmpFile, 'rb');
            stream_set_read_buffer($fh, 0);

            $j = 0;
            $carry = '';

            while (true) {
                $chunk = fread($fh, 1_048_576);
                if ($chunk === '' || $chunk === false) {
                    break;
                }

                if ($carry !== '') {
                    $chunk = $carry . $chunk;
                    $carry = '';
                }

                $len = strlen($chunk);
                $usable = $len - ($len & 1);

                if ($usable !== $len) {
                    $carry = $chunk[$len - 1];
                    $len = $usable;
                }

                for ($i = 0; $i < $len; $i += 2) {
                    $counts[$j++] += (ord($chunk[$i]) | (ord($chunk[$i + 1]) << 8));
                }
            }

            fclose($fh);
            unlink($tmpFile);

            $pending--;
        }

        $this->writeJson($outputPath, $counts);
    }

    protected function parseRange(string $inputPath, int $start, int $end): array
    {
        $buckets = array_fill(0, $this->pathCount, '');
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
            $unrolledLoopLimit = $lastNewLine - 720;

            while ($position < $unrolledLoopLimit) {
                for ($i = 0; $i < 6; $i++) {
                    $separatorPosition = strpos($chunk, ',', $position);

                    $dateId = $this->readDateId($chunk, $separatorPosition + 3);

                    $buckets[$this->pathIds[substr($chunk, $position, $separatorPosition - $position)]] .= chr($dateId & 0xFF) . chr(($dateId >> 8) & 0xFF);
                    $position = $separatorPosition + 52;
                }
            }

            while ($position < $lastNewLine) {
                $separatorPosition = strpos($chunk, ',', $position);
                if ($separatorPosition === false || $separatorPosition >= $lastNewLine) {
                    break;
                }

                $dateId = $this->readDateId($chunk, $separatorPosition + 3);

                $buckets[$this->pathIds[substr($chunk, $position, $separatorPosition - $position)]] .= chr($dateId & 0xFF) . chr(($dateId >> 8) & 0xFF);
                $position = $separatorPosition + 52;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $this->pathCount * $this->dateCount, 0);
        for ($p = 0; $p < $this->pathCount; $p++) {
            $bucket = $buckets[$p];
            if ($bucket === '') {
                continue;
            }

            $offset = $p * $this->dateCount;
            $len = strlen($bucket);
            for ($i = 0; $i < $len; $i += 2) {
                $did = ord($bucket[$i]) | (ord($bucket[$i + 1]) << 8);
                $counts[$offset + $did]++;
            }
        }

        return $counts;
    }

    protected function readDateId(string $chunk, int $dateStart): int
    {
        $year = (ord($chunk[$dateStart]) - 48) * 10 + (ord($chunk[$dateStart + 1]) - 48);

        $month = (ord($chunk[$dateStart + 3]) - 48) * 10 + (ord($chunk[$dateStart + 4]) - 48);

        $day = (ord($chunk[$dateStart + 6]) - 48) * 10 + (ord($chunk[$dateStart + 7]) - 48);

        $daysBeforeMonth = match ($month) {
            1 => 0,
            2 => 31,
            3 => 59,
            4 => 90,
            5 => 120,
            6 => 151,
            7 => 181,
            8 => 212,
            9 => 243,
            10 => 273,
            11 => 304,
            12 => 334,
            default => 0,
        };

        if ($month > 2 && (($year + 2000) % 4 === 0)) {
            $daysBeforeMonth++;
        }

        return $this->yearOffsets[$year] + $daysBeforeMonth + ($day - 1);
    }

    protected function writeJson(string $outputPath, array $counts): void
    {
        $writeBinary = fopen($outputPath, 'wb');
        stream_set_write_buffer($writeBinary, 1048576);
        fwrite($writeBinary, '{');

        $this->datePrefixes = [];
        for ($dates = 0; $dates < $this->dateCount; $dates++) {
            $this->datePrefixes[$dates] = "        \"20{$this->dates[$dates]}\": ";
        }

        $this->pathCount = count($this->paths);
        for ($path = 0; $path < $this->pathCount; $path++) {
            $this->escapedPaths[$path] = "\"\\/blog\\/" . str_replace('/', '\\/', $this->paths[$path]) . "\"";
        }

        for ($path = 0; $path < $this->pathCount; $path++) {
            $base = $path * $this->dateCount;

            $hasData = false;
            for ($d = 0; $d < $this->dateCount; $d++) {
                if ($counts[$base + $d] > 0) {
                    $hasData = true;
                    break;
                }
            }

            if (! $hasData) {
                continue;
            }

            $buffer = $this->firstPath ? "\n    " : ",\n    ";
            $this->firstPath = false;
            $buffer .= $this->escapedPaths[$path] . ": {\n";

            $this->firstDate = true;
            for ($d = 0; $d < $this->dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) {
                    continue;
                }

                $buffer .= $this->firstDate ? '' : ",\n";
                $this->firstDate = false;
                $buffer .= $this->datePrefixes[$d] . $count;
            }

            $buffer .= "\n    }";
            fwrite($writeBinary, $buffer);
        }

        fwrite($writeBinary, "\n}");
        fclose($writeBinary);
    }
}
