<?php

namespace Turbo\Foundation\Tests;

use PHPUnit\Framework\TestCase;
use Turbo\Foundation\MultiProcess;

/**
 * Class MultipleProcessTest
 * @package Turbo\Foundation\Tests
 */
class MultipleProcessTest extends TestCase
{
    public function testNormal()
    {
        $mp = new MultiProcess(5);

        $mp->setProducer(function () {
            sleep(1);
            var_dump('producer...');
            return date('Y-m-d H:i:s');
        });

        $mp->setConsumer(function ($pid, $consumerId, $data) {
            var_dump('consumer...');
            var_dump(sprintf("PID: %d, CID: %d, DATA: %s", $pid, $consumerId, json_encode([$data])));
        });

        $mp->run();

        $this->assertTrue(1 == 1);
    }

    public function testQueueSize()
    {
        $mp = new MultiProcess(5);

        $mp->setProducer(function () {
            sleep(1);
            var_dump('producer...');
            return date('Y-m-d H:i:s');
        });
        $mp->setConsumer(function ($pid, $consumerId, $data) {
            var_dump('consumer...');
            var_dump(sprintf("PID: %d, CID: %d, DATA: %s", $pid, $consumerId, json_encode([$data])));
        });

        $mp->run();
        var_dump($mp->getQueueSize());
        $this->assertTrue($mp->getQueueSize() > 1);
    }
}