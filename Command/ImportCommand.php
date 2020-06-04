<?php

namespace Bordeux\Bundle\GeoNameBundle\Command;


use Bordeux\Bundle\GeoNameBundle\Import\CountryImport;
use Bordeux\Bundle\GeoNameBundle\Import\ImportInterface;
use Bordeux\Bundle\GeoNameBundle\Repository\AdministrativeRepository;
use Doctrine\ORM\EntityManagerInterface;
use GuzzleHttp\Client;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Psr7\Uri;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerAwareTrait;

/**
 * Class VisitQueueCommand
 * @author Chris Bednarczyk <chris@tourradar.com>
 * @package TourRadar\Bundle\ApiBundle\Command\Queue
 */
class ImportCommand extends Command implements ContainerAwareInterface
{

    use ContainerAwareTrait;

    /**
     * @var EntityManagerInterface
    private $em;

    public function __construct(EntityManagerInterface $em, string $name = null)
    {
        parent::__construct($name);
        $this->em = $em;
    }
     */

    /**
     *
     */
    const PROGRESS_FORMAT = '%current%/%max% [%bar%] %percent:3s%% %elapsed:6s%/%estimated:-6s% Mem: %memory:6s% %message%';

    private function getContainer()
    {
        return $this->container;
    }

    private function getFilesToDownload($countries)
    {
        // download the files first, then import them.
        $filesToDownload = [
            'timeZones.txt',
            'admin1CodesASCII.txt',
            'admin2Codes.txt',
            'hierarchy.zip',
            'iso-languagecodes.txt',
            'countryInfo.txt'
        ];

        foreach (explode(',', $countries) as $country) {
            array_push($filesToDownload, trim($country) . '.zip');
        }

        return $filesToDownload;

    }
    /**
     * Configuration method
     */
    protected function configure()
    {


        $this
            ->setName('bordeux:geoname:import')
            ->addOption(
                'source',
                's',
                InputOption::VALUE_OPTIONAL,
                "Source URL for geoname download",
                'http://download.geonames.org/export/dump/'
            )
            ->addOption(
                'archive',
                'a',
                InputOption::VALUE_OPTIONAL,
                "Archive to GeoNames",
                'allCountries.zip'
            )
            ->addOption(
                'countries',
                'c',
                InputOption::VALUE_OPTIONAL,
                "Country codes, comma-delimited e.g. US, CA, MX",
                'MX'
            )
            ->addOption(
                'timezones',
                't',
                InputOption::VALUE_OPTIONAL,
                "Timezones file",
                'http://download.geonames.org/export/dump/timeZones.txt'
            )
            ->addOption(
                'admin1-codes',
                'a1',
                InputOption::VALUE_OPTIONAL,
                "Admin 1 Codes file",
                'http://download.geonames.org/export/dump/admin1CodesASCII.txt'
            )
            ->addOption(
                'hierarchy',
                'hi',
                InputOption::VALUE_OPTIONAL,
                "Hierarchy ZIP file",
                'http://download.geonames.org/export/dump/hierarchy.zip'
            )
            ->addOption(
                'admin2-codes',
                'a2',
                InputOption::VALUE_OPTIONAL,
                "Admin 2 Codes file",
                'http://download.geonames.org/export/dump/admin2Codes.txt'
            )
            ->addOption(
                'languages-codes',
                'lc',
                InputOption::VALUE_OPTIONAL,
                "Admin 2 Codes file",
                'http://download.geonames.org/export/dump/iso-languagecodes.txt'
            )
            ->addOption(
                'country-info',
                'ci',
                InputOption::VALUE_OPTIONAL,
                "Country info file",
                'http://download.geonames.org/export/dump/countryInfo.txt'
            )
            ->addOption(
                'download-dir',
                'o',
                InputOption::VALUE_OPTIONAL,
                "Download dir",
                null
            )
            ->addOption(
                "skip-admin1",
                null,
                InputOption::VALUE_OPTIONAL,
                '',
                false)
            ->addOption(
                "skip-admin2",
                null,
                InputOption::VALUE_NONE,
                'Skip importing admin2')
            ->addOption(
                "skip-geoname",
                null,
                InputOption::VALUE_NONE,
                'Skip the geoname import')
            ->addOption(
                "skip-hierarchy",
                null,
                InputOption::VALUE_OPTIONAL,
                '',
                false
            )
            ->setDescription('Import GeoNames');
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {

        $io = new SymfonyStyle($input, $output);
        $downloadDir = $input->getOption('download-dir') ?: $this->getContainer()->getParameter("kernel.cache_dir") . DIRECTORY_SEPARATOR . 'bordeux/geoname';

        !file_exists($downloadDir) && mkdir($downloadDir, 0700, true);
        $downloadDir = realpath($downloadDir);

        $filesToDownload = $this->getFilesToDownload($input->getOption('countries'));
        foreach ($filesToDownload as $filename) {
            $remoteFile = $input->getOption('source') . $filename;
            $localfile = $downloadDir . DIRECTORY_SEPARATOR . basename($filename);
            if (file_exists($localfile)) {
                $io->warning("$localfile in cache");
            } else {
                $this->downloadWithProgressBar(
                    $remoteFile,
                    $localfile,
                    $output
                )->wait();
                $io->success($localfile . " downloaded.");
            }
        }
        $io->writeln('Finished downloading');

        if (!$input->getOption("skip-admin1")) {
            // admin1
            $admin1 = $input->getOption('admin1-codes');
            $admin1Local = $downloadDir . DIRECTORY_SEPARATOR . basename($admin1);

            $this->downloadWithProgressBar(
                $admin1,
                $admin1Local,
                $output
            )->wait();
            $output->writeln('');

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.administrative"),
                $admin1Local,
                "Importing administrative 1 to Admininstrative ",
                $output
            )->wait();

            $output->writeln('');
        }

        //timezones

        if ($timezones = $input->getOption('timezones'))
        {
            $timezonesLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($timezones);

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.timezone"),
                $timezonesLocal,
                "Importing timezones",
                $output
            )->wait();

            $output->writeln('');

        }


        // country-info
        if ($countryInfo = $input->getOption('country-info'))
        {
            $countryInfoLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($countryInfo);

            //countries import
            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.country"),
                $countryInfoLocal,
                "Importing Countries",
                $output
            )->wait();

        } else {
            $output->write("Skipping country-info");
        }


        //importing

        $output->writeln('');




        if (!$input->getOption("skip-admin2")) {
            $admin2 = $input->getOption('admin2-codes');
            $admin2Local = $downloadDir . DIRECTORY_SEPARATOR . basename($admin2);

            $this->downloadWithProgressBar(
                $admin2,
                $admin2Local,
                $output
            )->wait();
            $output->writeln('');

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.administrative"),
                $admin2Local,
                "Importing administrative 2",
                $output
            )->wait();
            $output->writeln('Administative Table Loaded, Geoname IDs are invalid until Geonames is loaded.');
        }

        // moved above admin and countries for testing!
        if (!$input->getOption("skip-geoname")) {
            // archive
            $archive = $input->getOption('archive');
            $archiveLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($archive);

            $this->downloadWithProgressBar(
                $archive,
                $archiveLocal,
                $output
            )->wait();
            $output->writeln('');

            $importService = $this->getContainer()->get("bordeux.geoname.import.geoname");

            $importService->setFilters([
                // 'featureCode' => ['PPL']
                // 'featureClass' => ['P', 'A', 'V'],
            ]);


            $this->importWithProgressBar(
                $importService,
                $archiveLocal,
                "Importing GeoNames",
                $output,
                1000
            )->wait();


            $output->writeln("");
        }


        if (!$input->getOption("skip-hierarchy")) {
            // archive
            $archive = $input->getOption('hierarchy');
            $archiveLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($archive);

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.hierarchy"),
                $archiveLocal,
                "Importing Hierarchy",
                $output,
                1000
            )->wait();


            $output->writeln("");
        }


        $output->writeln("");


        $output->writeln("Imported successfully! Thank you :) ");

        return 0;

    }

    /**
     * @param ImportInterface $importer
     * @param string $file
     * @param string $message
     * @param OutputInterface $output
     * @param int $steps
     * @return \GuzzleHttp\Promise\Promise|\GuzzleHttp\Promise\PromiseInterface
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function importWithProgressBar(ImportInterface $importer, $file, $message, OutputInterface $output, $steps = 100)
    {
        // get the number of lines
        $lineCount = exec("wc -l  < $file");
        $steps = $lineCount;
        $progress = new ProgressBar($output, $lineCount);
        $progress->setFormat(self::PROGRESS_FORMAT);
        $progress->setMessage($message . ' ' . $file);
        $progress->setRedrawFrequency(1);
        $progress->start();

        return $importer->import(
            $file,
            function ($percent) use ($progress, $steps) {
                $progress->setProgress((int)($percent * $steps));
            }
        )->then(function () use ($progress) {
            $progress->finish();
        });
    }


    /**
     * @param string $url
     * @param string $saveAs
     * @param OutputInterface $output
     * @return \GuzzleHttp\Promise\PromiseInterface
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function downloadWithProgressBar($url, $saveAs, OutputInterface $output)
    {
        if (file_exists($saveAs)) {
            $output->writeln($saveAs . " exists in the cache.");

            $promise = new Promise();
            $promise->then(
            // $onFulfilled
                function ($value) {
                    echo 'The promise was fulfilled.';
                },
                // $onRejected
                function ($reason) {
                    echo 'The promise was rejected.';
                }
            );

            $promise->resolve("In cache!");
            return $promise;
        }

        $progress = new ProgressBar($output, 100);
        $progress->setFormat(self::PROGRESS_FORMAT);
        $progress->setMessage("Start downloading {$url}");
        $progress->setRedrawFrequency(1);
        $progress->start();

        return $this->download(
            $url,
            $saveAs,
            function ($percent) use ($progress) {
                $progress->setProgress((int)($percent * 100));
            }
        )->then(function () use ($progress) {
            $progress->finish();
        });

    }


    /**
     * @param string $url
     * @param string $output
     * @param callable $progress
     * @return \GuzzleHttp\Promise\PromiseInterface
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function download($url, $saveAs, callable $progress)
    {
        $client = new Client([]);

        $promise = $client->getAsync(
            new Uri($url),
            [
                'progress' => function ($totalSize, $downloadedSize) use ($progress) {
                    $totalSize && is_callable($progress) && $progress($downloadedSize / $totalSize);
                },
                'save_to' => $saveAs
            ]
        );

        return $promise;
    }
}
