<?php

declare(strict_types=1);

namespace Kvazar\Index;

use Manticoresearch\Client;
use Manticoresearch\Index;

class Manticore
{
    private \Manticoresearch\Client $_client;
    private \Manticoresearch\Index  $_index;

    private const OP_KEVA_NAMESPACE = 1;
    private const OP_KEVA_PUT       = 2;
    private const OP_KEVA_DELETE    = 3;
    private const OP_HASH160        = 4;
    private const OP_RETURN         = 5;
    private const OP_DUP            = 6;
    private const OP_NOP            = 7;

    public function __construct(
        ?string $name = 'kvazar',
        ?array  $meta = [],
        ?string $host = '127.0.0.1',
        ?int    $port = 9308
    ) {
        $this->_client = new Client(
            [
                'host' => $host,
                'port' => $port,
            ]
        );

        $this->_index = $this->_client->index(
            $name
        );

        $this->_index->create(
            [
                'crc32namespace' =>
                [
                    'type' => 'int'
                ],
                'crc32transaction' =>
                [
                    'type' => 'int'
                ],
                'crc32key' =>
                [
                    'type' => 'int'
                ],
                'crc32value' =>
                [
                    'type' => 'int'
                ],
                'operation' =>
                [
                    'type' => 'int'
                ],
                'time' =>
                [
                    'type' => 'int'
                ],
                'size' =>
                [
                    'type' => 'int'
                ],
                'block' =>
                [
                    'type' => 'int'
                ],
                'namespace' =>
                [
                    'type' => 'text'
                ],
                'transaction' =>
                [
                    'type' => 'text'
                ],
                'key' =>
                [
                    'type' => 'text'
                ],
                'value' =>
                [
                    'type' => 'text'
                ]
            ],
            $meta,
            true
        );
    }

    public function add(
        int    $time,
        int    $size,
        int    $block,
        string $namespace,
        string $transaction,
        string $operation,
        string $key,
        string $value
    ) {
        return $this->_index->addDocument(
            [
                'crc32namespace'   => $this->_crc32(
                    $namespace
                ),
                'crc32transaction' => $this->_crc32(
                    $transaction
                ),
                'crc32key'         => $this->_crc32(
                    $key
                ),
                'crc32value'       => $this->_crc32(
                    $value
                ),
                'operation'        => $this->_operation(
                    $operation
                ),
                'time'             => $time,
                'size'             => $size,
                'block'            => $block,
                'namespace'        => $namespace,
                'transaction'      => $transaction,
                'key'              => $key,
                'value'            => $value
            ]
        );
    }

    public function get(
        ?string $search = '',
        ?array  $filter = [],
        ?array  $sort   = ['id' => 'desc'],
        ?int    $offset = 0,
        ?int    $limit  = 2
    ): array
    {
        $records = [];

        $search = $this->_index->search(
            $search
        );

        foreach ($filter as $key => $value)
        {
            $search->filter(
                $key,
                $value
            );
        }

        foreach ($sort as $key => $value)
        {
            $search->sort(
                $key,
                $value
            );
        }

        $search->offset(
            $offset
        );

        $search->limit(
            $limit
        );

        foreach ($search->get() as $record)
        {
            $records[$record->getId()] =
            [
                'time'        => $record->get('time'),
                'size'        => $record->get('size'),
                'block'       => $record->get('block'),
                'namespace'   => $record->get('namespace'),
                'transaction' => $record->get('transaction'),
                'key'         => $record->get('key'),
                'value'       => $record->get('value')
            ];
         }

        return $records;
    }

    public function drop(?bool $silent = false)
    {
        return $this->_index->drop(
            $silent
        );
    }

    public function optimize(?bool $sync = false)
    {
        return $this->_index->optimize(
            $sync
        );
    }

    private function _crc32(mixed $value): int
    {
        return crc32(
            $value
        );
    }

    private function _operation(string $value): int
    {
        switch ($value)
        {
            case 'OP_KEVA_NAMESPACE':
                return self::OP_KEVA_NAMESPACE;

            case 'OP_KEVA_PUT':
                return self::OP_KEVA_PUT;

            case 'OP_KEVA_DELETE':
                return self::OP_KEVA_DELETE;

            case 'OP_HASH160':
                return self::OP_HASH160;

            case 'OP_RETURN':
                return self::OP_RETURN;

            case 'OP_DUP':
                return self::OP_DUP;

            case 'OP_NOP':
                return self::OP_NOP;

            default:
                return 0;
        }
    }
}