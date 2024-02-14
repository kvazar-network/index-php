<?php

declare(strict_types=1);

namespace Kvazar\Index;

use Manticoresearch\Client;
use Manticoresearch\Index;

class Manticore
{
    private \Manticoresearch\Client $_client;
    private \Manticoresearch\Index  $_index;

    private const OP_KEVA           = 100;
    private const OP_KEVA_NAMESPACE = 110;
    private const OP_KEVA_PUT       = 120;
    private const OP_KEVA_DELETE    = 130;
    private const OP_HASH160        = 200;
    private const OP_RETURN         = 300;
    private const OP_DUP            = 400;
    private const OP_NOP            = 500;

    private const TYPE_TEXT         = 100;
    private const TYPE_BIN          = 200;
    private const TYPE_JSON         = 300;
    private const TYPE_XML          = 400;
    private const TYPE_BASE_64      = 500;

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
                'crc32_namespace' =>
                [
                    'type' => 'int'
                ],
                'crc32_transaction' =>
                [
                    'type' => 'int'
                ],
                'crc32_key' =>
                [
                    'type' => 'int'
                ],
                'crc32_value' =>
                [
                    'type' => 'int'
                ],
                'type_key' =>
                [
                    'type' => 'int'
                ],
                'type_value' =>
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
        mixed  $key,
        mixed  $value
    ) {
        return $this->_index->addDocument(
            [
                'crc32_namespace'   => $this->_crc32(
                    $namespace
                ),
                'crc32_transaction' => $this->_crc32(
                    $transaction
                ),
                'crc32_key'         => $this->_crc32(
                    $key
                ),
                'crc32_value'       => $this->_crc32(
                    $value
                ),

                'type_key'          => $this->_type(
                    $key,
                    $typeKey
                ),
                'type_value'        => $this->_type(
                    $value,
                    $typeValue
                ),

                'operation'         => $this->_operation(
                    $operation
                ),

                'time'              => $time,
                'size'              => $size,
                'block'             => $block,
                'namespace'         => $namespace,
                'transaction'       => $transaction,

                // Manticore can't store binary data, convert to Base64 string
                'key'               => $typeKey   === self::TYPE_BIN ? base64_encode($key)   : $key,
                'value'             => $typeValue === self::TYPE_BIN ? base64_encode($value) : $value,
            ]
        );
    }

    public function get(
        ?string $query  = '',
        ?array  $filter = [],
        ?array  $sort   = ['id' => 'desc'],
        ?int    $offset = 0,
        ?int    $limit  = 10
    ): array
    {
        $records = [];

        $search = $this->_index->search(
            $query
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

                // Raw data stored as Base64 string, convert back
                'key'         => $record->get('key_type')   === self::TYPE_BIN ? base64_decode($record->get('key'))   : $record->get('key'),
                'value'       => $record->get('value_type') === self::TYPE_BIN ? base64_decode($record->get('value')) : $record->get('value'),
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

    private function _type(mixed $value, ?int &$type = null): int
    {
        switch (true)
        {
            case false === mb_detect_encoding((string) $value, null, true):
                return $type = self::TYPE_BIN;

            case base64_encode((string) base64_decode((string) $value, true)) === $value:
                return $type = self::TYPE_BASE_64;

            case null !== json_decode((string) $value, null, 2147483647):
                return $type = self::TYPE_JSON;

            case false !== simplexml_load_string((string) $value):
                return $type = self::TYPE_XML;

            default:
                return $type = self::TYPE_TEXT;
        }
    }
}