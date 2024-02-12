<?php

declare(strict_types=1);

namespace Kvazar\Index;

class Manticore
{
    private \Manticoresearch\Client $_client;

    private $_index;

    public function __construct(
        ?string $name = 'kvazar',
        ?string $host = '127.0.0.1',
        ?int    $port = 9308,
        ?bool   $drop = false
    )
    {
        $this->_client = new \Manticoresearch\Client(
            [
                'host' => $host,
                'port' => $port,
            ]
        );

        $this->_index = $this->_client->index(
            $name
        );

        if ($drop)
        {
            $this->_index->drop(
                true
            );
        }

        if (!$this->_index->status())
        {
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
                    'txid' =>
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
                ]
            );
        }
    }
}