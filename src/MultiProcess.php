<?php

namespace Turbo\Foundation;

/**
 * Class MultiProcess
 * @package Turbo\Foundation
 */
class MultiProcess
{
    /**
     * Producer process type
     */
    const P_TYPE_PRODUCER  = 1;

    /**
     * Consumer process type
     */
    const P_TYPE_CONSUMER  = 2;

    /**
     * Normal message
     */
    const M_TYPE_NORMAL   = 1;

    /**
     * End message
     */
    const M_TYPE_END      = 99;

    /**
     * Producer process number
     *
     * @var int
     */
    private $producerWorkerNum;

    /**
     * Consumer process number, must be larger than zero
     *
     * @var int
     */
    private $consumerWorkerNum;

    /**
     * Producer
     *
     * @var callable|array
     */
    private $producer;

    /**
     * Consumer
     *
     * @var callable
     */
    private $consumer;

    /**
     * Producer process list
     *
     * @var array
     */
    private $producers = [];

    /**
     * Consumer process list
     *
     * @var array
     */
    private $consumers = [];

    /**
     * System V message queue handler
     *
     * @var resource
     */
    private $queue;

    /**
     * Main process quit flag
     *
     * @var bool
     */
    private $quit = false;

    /**
     * Config
     *
     * @var array
     */
    private $config = [
        'name'      => 'TurboMultiProcess',
        'restart'   => false
    ];

    /**
     * Create a new MultipleProcess instance
     *
     * @param  int $consumerNum
     * @param  int $producerNum
     * @return void
     */
    public function __construct($consumerNum, $producerNum = 0)
    {
        $this->consumerWorkerNum = $consumerNum;
        $this->producerWorkerNum = $producerNum;

        assert($this->consumerWorkerNum > 0);
    }

    /**
     * @return $this
     */
    public function setAutoRestart()
    {
        $this->config['restart'] = true;

        return $this;
    }

    /**
     * Set producer
     *
     * @param  callable|array $producer
     * @return void
     */
    public function setProducer($producer)
    {
        if (is_callable($producer)) {
            $this->producer = $producer;
            if ($this->producerWorkerNum <= 0) {
                $this->producerWorkerNum = 1;
            }

        } else {
            $this->producerWorkerNum = 1;
            $this->producer    = function() use (&$producer) {
                return $producer ? array_shift($producer) : false;
            };
        }
    }

    /**
     * Set consumer
     *
     * @param  callable $consumer
     * @return void
     */
    public function setConsumer($consumer)
    {
        $this->consumer = $consumer;
    }

    /**
     * Set config
     *
     * @param  array $config
     * @return void
     */
    public function setConfig($config)
    {
        $this->config = array_merge($this->config, $config);
    }

    /**
     * Set process title
     *
     * @param  string $type
     * @return void
     */
    private function setProcessTitle($type)
    {
        if (function_exists('cli_set_process_title')) {
            @cli_set_process_title($this->config['name'] . ': ' . $type);
        }
    }

    /**
     * Create producer process
     *
     * @param  int $index process index
     * @return void
     */
    private function createProducerProcess($index)
    {
        $pid = pcntl_fork();
        assert($pid != -1);

        if ($pid == 0) {
            // in child

            // restore signal handler
            pcntl_signal(SIGINT,  SIG_DFL);
            pcntl_signal(SIGTERM, SIG_DFL);

            // set producer process title
            $this->setProcessTitle('producer');

            // current pid
            $currentPid = posix_getpid();

            // producer loop
            while (true) {

                $data = call_user_func($this->producer, $currentPid, $index);

                if ($data === false) {
                    break;
                }

                if ($data !== NULL) {

                    if (! $data instanceof Iterator) {
                        $data = [$data];
                    }

                    foreach ($data as $item) {
                        if (!msg_send($this->queue, self::M_TYPE_NORMAL, $item, true, true, $errcode)) {
                        }
                    }
                }
            }

            // producer exit!
            exit(0);
        }

        // in parent
        $this->producers[$index] = $pid;
    }

    /**
     * Create consumer process
     *
     * @param  int $index process index
     * @return void
     */
    private function createConsumerProcess($index)
    {
        $pid = pcntl_fork();
        assert($pid != -1);

        if ($pid == 0) {
            // in child

            // restore signal handler
            pcntl_signal(SIGINT,  SIG_DFL);
            pcntl_signal(SIGTERM, SIG_DFL);

            // set consumer process title
            $this->setProcessTitle('consumer');

            // current pid
            $currentPid = posix_getpid();

            if (!$this->producerWorkerNum) {
                // no producer!
                call_user_func($this->consumer, $currentPid, $index);

            } else {
                $maxSize = msg_stat_queue($this->queue)['msg_qbytes'];

                // consumer loop
                while (true) {

                    if (!msg_receive($this->queue, 0, $messageType, $maxSize, $data, true, 0, $errcode)) {
                        continue;
                    }

                    if ($messageType === self::M_TYPE_NORMAL) {
                        call_user_func($this->consumer, $currentPid, $index, $data);
                    } else {
                        break;
                    }
                }
            }

            // consumer exit!
            exit(0);
        }

        // in parent
        $this->consumers[$index] = $pid;
    }

    /**
     * Main porcess quit
     *
     * @return void
     */
    private function mainExit()
    {
        $this->quit = true;
    }

    /**
     * Judge if need to restart
     *
     * @param  int $status
     * @return bool
     */
    private function isNeedRestart($status)
    {
        return (!pcntl_wifexited($status) || pcntl_wexitstatus($status)) && !$this->quit && $this->config['restart'];
    }

    /**
     * Child process quit
     *
     * @return void
     */
    private function childExit()
    {
        while ( ($pid = pcntl_wait($status, WNOHANG)) > 0) {

            // if exited is consumer
            if ( ($index = array_search($pid, $this->consumers)) !== false) {
                if ($this->isNeedRestart($status)) {
                    $this->createConsumerProcess($index);
                } else {
                    unset($this->consumers[$index]);
                    if (!$this->consumers) {
                        // no consumers, main quit
                        $this->mainExit();
                    }
                }

                continue;
            }

            // if exited is producer
            if ( ($index = array_search($pid, $this->producers)) !== false) {
                if ($this->isNeedRestart($status)) {
                    $this->createProducerProcess($index);
                } else {
                    unset($this->producers[$index]);
                    if (!$this->producers && $this->queue) {
                        for ($i = 0; $i < $this->consumerWorkerNum; ++$i) {
                            msg_send($this->queue, self::M_TYPE_END, NULL);
                        }
                    }
                }
            }
        }
    }

    /**
     * Start multiple process!
     *
     * @param array $config
     * @return void
     */
    public function run($config = [])
    {
        $this->setConfig($config);

        // signal handler
        pcntl_signal(SIGINT,  [$this, 'mainExit']);
        pcntl_signal(SIGTERM, [$this, 'mainExit']);
        pcntl_signal(SIGCHLD, [$this, 'childExit']);

        // set main process title
        $this->setProcessTitle('main');

        // if have producer
        if ($this->producerWorkerNum) {
            // create message queue
            $this->queue = msg_get_queue(posix_getpid());

            // create producer process
            for ($i = 0; $i < $this->producerWorkerNum; ++$i) {
                $this->createProducerProcess($i);
            }
        }

        // create consumer process
        for ($i = 0; $i < $this->consumerWorkerNum; ++$i) {
            $this->createConsumerProcess($i);
        }

        // main loop
        while (!$this->quit) {
            sleep(1000);
            pcntl_signal_dispatch();
        }

        // kill all producers
        foreach ($this->producers as $pid) {
            posix_kill($pid, SIGTERM);
        }

        // kill all consumers
        foreach ($this->consumers as $pid) {
            posix_kill($pid, SIGTERM);
        }

        // remove message queue if need
        if ($this->queue) {
            msg_remove_queue($this->queue);
        }
    }

    /**
     * Execute task with multiple process
     *
     * @param  array    $tasks
     * @param  int      $maxProcessNum
     * @param  callable $callback
     * @return void
     */
    public static function execute($tasks, $maxProcessNum, $callback)
    {
        $executeChildTask = function ($task) use ($callback) {
            if (pcntl_fork() == 0) {
                $callback($task);
                exit(0);
            }
        };

        for ($i = 0, $num = min($maxProcessNum, count($tasks)); $i < $num; ++$i) {
            $executeChildTask(array_shift($tasks));
        }

        while (pcntl_wait($status) > 0) {
            if ($tasks) {
                $executeChildTask(array_shift($tasks));
            }
        }
    }
}