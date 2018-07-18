<?php
namespace Sellastica\Console\Command;

/**
 * Consumes messages from all queues for current virtual host
 * This command is defined in the crontab and runs each minute
 */
class SchedulerRun extends \Symfony\Component\Console\Command\Command
{
	/** @var \Sellastica\Scheduler\Model\Scheduler @inject */
	public $scheduler;
	/** @var \Sellastica\Entity\EntityManager @inject */
	public $em;


	protected function configure()
	{
		$this->setName('scheduler:run')
			->setDescription('Runs Scheduler')
			->setDefinition(
				new \Symfony\Component\Console\Input\InputDefinition([
					new \Symfony\Component\Console\Input\InputOption('jobId', null, \Symfony\Component\Console\Input\InputOption::VALUE_REQUIRED),
					new \Symfony\Component\Console\Input\InputOption('projectId', null, \Symfony\Component\Console\Input\InputOption::VALUE_REQUIRED),
				])
			);
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
			if (!$jobId = $input->getOption('jobId')) {
				$this->scheduler->clearOldLogEntries();
				$this->scheduler->run();
			} else {
				/** @var \Sellastica\Scheduler\Entity\SchedulerJobSetting $jobSetting */
				if (!$jobSetting = $this->em->getRepository(\Sellastica\Scheduler\Entity\SchedulerJobSetting::class)
					->find($jobId)) {
					$output->writeLn('<error>Job not found</error>');
				}

				$this->scheduler->clearOldLogEntries();
				$logMessages = $this->scheduler->runJob($jobSetting, true);
				foreach ($logMessages as $logMessage) {
					$output->writeLn('<info>' . $logMessage . '</info>');
				}
			}

			return 0;
		} catch (\Throwable $e) {
			$output->writeLn('<error>' . $e->getMessage() . '</error>');
			return 1;
		}
	}
}
