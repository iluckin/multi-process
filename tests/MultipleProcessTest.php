<?php

namespace Turbo\Foundation\Tests;

use PHPUnit\Framework\TestCase;
use Turbo\Foundation\MultipleProcess;

/**
 * Class MultipleProcessTest
 * @package Turbo\Foundation\Tests
 */
class MultipleProcessTest extends TestCase
{
    public function testNormal()
    {
        $mp = new MultipleProcess(5);

        $mp->setProducer(function () {
            var_dump('producer...');
            return date('Y-m-d H:i:s');
        });

        $mp->setConsumer(function ($pid, $consumerId, $data) {
            var_dump('consumer...');
            var_dump(sprintf("PID: %d, CID: %d, DATA: %s", $pid, $consumerId, json_encode([$data])));
        });

        $mp->run();
    }
}