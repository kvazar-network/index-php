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

    private const TYPE_NULL         = 0;
    private const TYPE_BOOL         = 1;
    private const TYPE_INT          = 100;
    private const TYPE_FLOAT        = 110;
    private const TYPE_STRING       = 200;
    private const TYPE_BIN          = 210;
    private const TYPE_JSON         = 220;
    private const TYPE_XML          = 230;
    private const TYPE_BASE_64      = 240;
    private const TYPE_ARRAY        = 300;
    private const TYPE_OBJECT       = 400;

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

        // Manticore can store text data only, convert non-string types to Base64
        if (self::TYPE_STRING !== $typeKey = $this->_type($key))
        {
            if (false === $key = json_encode($key))
            {
                throw new Exception();
            }
        }

        if (self::TYPE_STRING !== $typeValue = $this->_type($value))
        {
            if (false === $value = json_encode($value))
            {
                throw new Exception();
            }
        }

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

                'type_key'          => $typeKey,
                'type_value'        => $typeValue,

                'operation'         => $this->_operation(
                    $operation
                ),

                'time'              => $time,
                'size'              => $size,
                'block'             => $block,
                'namespace'         => $namespace,
                'transaction'       => $transaction,

                'key'               => $key,
                'value'             => $value,
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
            // Raw data stored as JSON encoded string
            if ($record->get('key_type') === self::TYPE_STRING)
            {
                $key = $record->get('key');
            }

            else
            {
                $key = json_decode(
                    $record->get('key')
                );
            }

            if ($record->get('value_type') === self::TYPE_STRING)
            {
                $value = $record->get('value');
            }

            else
            {
                $value = json_decode(
                    $record->get('value')
                );
            }

            $records[$record->getId()] =
            [
                'time'        => $record->get('time'),
                'size'        => $record->get('size'),
                'block'       => $record->get('block'),
                'namespace'   => $record->get('namespace'),
                'transaction' => $record->get('transaction'),

                'key'         => $key,
                'value'       => $value,
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

    private function _crc32(string $value): int
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

    private function _type(mixed $value): int
    {
        switch (true)
        {
            case is_int($value):
                return self::TYPE_INT;

            case is_float($value):
                return self::TYPE_FLOAT;

            case is_bool($value):
                return self::TYPE_BOOL;

            case is_null($value):
                return self::TYPE_NULL;

            case is_array($value):
                return self::TYPE_ARRAY;

            case is_object($value):
                return self::TYPE_OBJECT;

            case false === mb_detect_encoding((string) $value, null, true):
                return self::TYPE_BIN;

            case base64_encode((string) base64_decode((string) $value, true)) === $value:
                return self::TYPE_BASE_64;

            case json_encode((string) json_decode((string) $value)) === $value:
                return self::TYPE_JSON;

            case false !== @simplexml_load_string((string) $value):
                return self::TYPE_XML;

            default:
                return self::TYPE_STRING;
        }
    }
}