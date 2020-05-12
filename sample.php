<?php

require __DIR__ . '/vendor/autoload.php';

$mp = new \Turbo\Foundation\MultipleProcess(2);

$mp->setProducer(function () {
    return 1000001;
});

$mp->setConsumer(function ($pid, $consumerId, $data) use (&$a) {

});

$mp->run();