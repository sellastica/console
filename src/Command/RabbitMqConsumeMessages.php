<?php
namespace Sellastica\Console\Command;

/**
 * Consumes messages from all queues for current virtual host
 * This command is defined in the crontab and runs each minute
 */
class RabbitMqConsumeMessages extends \Symfony\Component\Console\Command\Command
{
	/** If last run end is null, we run next consumers after this interval */
	private const UNKNOWN_LAST_RUN_INTERVAL = 'PT5M';

	/** @var \Gamee\RabbitMQ\Consumer\ConsumerFactory @inject */
	public $consumerFactory;
	/** @var \Nette\DI\Container @inject */
	public $container;
	/** @var \Sellastica\Project\Model\SettingsAccessor @inject */
	public $settingsAccessor;
	/** @var \Sellastica\Monolog\Logger @inject */
	public $logger;
	/** @var \Sellastica\Entity\EntityManager @inject */
	public $em;


	protected function configure()
	{
		$this->setName('bunny:consume')
			->setDescription('Consume RabbitMQ messages from current virtual host');
		$this->addArgument('consumer', \Symfony\Component\Console\Input\InputArgument::OPTIONAL, 'Consumer name');
		$this->addArgument('seconds', \Symfony\Component\Console\Input\InputArgument::OPTIONAL, 'Number of seconds to run each consumer');
	}

	/**
	 * @param \Symfony\Component\Console\Input\InputInterface $input
	 * @param \Symfony\Component\Console\Output\OutputInterface $output
	 * @return int
	 */
	protected function execute(
		\Symfony\Component\Console\Input\InputInterface $input,
		\Symfony\Component\Console\Output\OutputInterface $output
	)
	{
		try {
			if ((int)$this->settingsAccessor->getSetting('project.rabbitmq_active') !== 1) {
				throw new \RuntimeException('RabbitMQ is disabled in the settings table');
			}

			if (!$this->hasToRun()) {
				throw new \RuntimeException(sprintf(
					'RabbitMQ is stopped till %s or till previous consumers end',
					$this->getNextRunStart() ? $this->getNextRunStart()->format('Y-m-d H:i:s') : 0
				));
			}

			//log start
			$this->setLastRunStart((new \DateTime())->format('Y-m-d H:i:s'));
			$this->setLastRunEnd(null);
			$this->em->flush();

			$consumerNames = $input->getArgument('consumer')
				? [$input->getArgument('consumer')]
				: $this->container->getParameters()['rabbitmq']['consumers'] ?? [];

			foreach ($consumerNames as $consumerName) {
				$consumer = $this->consumerFactory->getConsumer($consumerName);
				$consumer->consume($input->getArgument('seconds') ?: 1);
				$consumer->getQueue()->getConnection()->getBunnyClient()->stop();
				$output->writeLn("<info>$consumerName OK</info>");
			}

			//log end
			$this->setLastRunEnd((new \DateTime())->format('Y-m-d H:i:s'));
			$this->em->flush();

			return 0;
		} catch (\Throwable $e) {
			$output->writeLn('<error>' . get_class($e) . ': ' . $e->getMessage() . '</error>');
			$this->logger->exception($e);
			return 1;
		}
	}

	/**
	 * @return bool
	 */
	private function hasToRun(): bool
	{
		$lastRunStart = $this->getLastRunStart();
		$lastRunEnd = $this->getLastRunEnd();

		if (!$lastRunStart) { //never ran before
			return true;
		} elseif (!$lastRunEnd) {
			//end timestamp may not be logged because of some server error
			//in that case, we cannot disable rabbitMQ for ever!
			//so, we run rabbitMQ, if last run was min. XX minutes before
			$nextStart = clone $lastRunStart;
			$nextStart->add(new \DateInterval(self::UNKNOWN_LAST_RUN_INTERVAL));
			if ($nextStart < new \DateTime('now')) { //if last start was earlier then XX minutes before
				return true;
			} else {
				return false;
			}
		}

		return true;
	}

	/**
	 * @return \DateTime|null
	 */
	private function getLastRunStart(): ?\DateTime
	{
		$lastRunDate = $this->settingsAccessor->getSetting('project.rabbitmq_last_run_start');
		return $lastRunDate ? new \DateTime($lastRunDate) : null;
	}

	/**
	 * @param $start
	 */
	private function setLastRunStart($start): void
	{
		$this->settingsAccessor->get()->saveSettingValue('rabbitmq_last_run_start', 'project', $start);
	}

	/**
	 * @param $end
	 */
	private function setLastRunEnd($end): void
	{
		$this->settingsAccessor->get()->saveSettingValue('rabbitmq_last_run_end', 'project', $end);
	}

	/**
	 * @return \DateTime|null
	 */
	private function getLastRunEnd(): ?\DateTime
	{
		$lastRunDate = $this->settingsAccessor->getSetting('project.rabbitmq_last_run_end');
		return $lastRunDate ? new \DateTime($lastRunDate) : null;
	}

	/**
	 * @return \DateTime
	 */
	private function getNextRunStart(): \DateTime
	{
		$lastRunStart = $this->getLastRunStart();
		return $lastRunStart ? $lastRunStart->add(new \DateInterval(self::UNKNOWN_LAST_RUN_INTERVAL)) : null;
	}
}
