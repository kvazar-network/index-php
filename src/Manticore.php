<?php

declare(strict_types=1);

namespace Kvazar\Index;

use Manticoresearch\Client;
use Manticoresearch\Index;

class Manticore
{
    private \Manticoresearch\Client $_client;
    private \Manticoresearch\Index  $_index;

    public function __construct(
        ?string $name = 'kvazar',
        ?array  $meta = [],
        ?string $host = '127.0.0.1',
        ?int    $port = 9308
    )
    {
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
        int    $block,
        string $namespace,
        string $transaction,
        string $key,
        string $value
    ): int
    {
        $crc32transaction = crc32(
            $transaction
        );

        if (!$this->_index->getDocumentById($crc32transaction))
        {
            $this->_index->addDocument(
                [
                    'block'            => $block,
                    'namespace'        => $namespace,
                    'transaction'      => $transaction,
                    'key'              => $key,
                    'value'            => $value
                ],
                $crc32transaction
            );
        }

        return $crc32transaction;
    }

    public function drop(
        ?bool $silent = false
    )
    {
        return $this->_index->drop(
            $silent
        );
    }
}