<?php

namespace Bordeux\Bundle\GeoNameBundle\Command;


use Bordeux\Bundle\GeoNameBundle\Import\AdministrativeImport;
use Bordeux\Bundle\GeoNameBundle\Import\CountryImport;
use Bordeux\Bundle\GeoNameBundle\Import\HierarchyImport;
use Bordeux\Bundle\GeoNameBundle\Import\ImportInterface;

use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerAwareTrait;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBagInterface;
use Symfony\Component\HttpClient\HttpClient;

use GuzzleHttp\Client;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Psr7\Uri;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * Class VisitQueueCommand
 * @author Chris Bednarczyk <chris@tourradar.com>
 * @package TourRadar\Bundle\ApiBundle\Command\Queue
 */
class ImportCommand extends Command implements ContainerAwareInterface
{

    private string $endpoint;

    public function __construct(
        private ParameterBagInterface $bag,
        private CountryImport $countryImport,
        string $name = null)
    {
        parent::__construct($name);
    }

    use ContainerAwareTrait;

    /**
     *
     */
    const PROGRESS_FORMAT = '%current%/%max% [%bar%] %percent:3s%% %elapsed:6s%/%estimated:-6s% Mem: %memory:6s% %message%';

    /**
     * Configuration method
     */
    protected function configure()
    {

        $this
            ->setName('bordeux:geoname:import')
            ->setDescription('Import GeoNames into NestedTree format, flush after each hierarchy.')
            ->addOption('endpoint', 'url', InputOption::VALUE_OPTIONAL, 'Geonames endpoint', 'http://download.geonames.org/export/dump/')
            ->addOption(
                'download-dir',
                'o',
                InputOption::VALUE_OPTIONAL,
                "Download dir",
                null
            )

            ->addOption('countries', 'ci', InputOption::VALUE_NEGATABLE, 'import counries-info', false)
            ->addOption('timezones', 'tz', InputOption::VALUE_NEGATABLE, 'import timezones', false)
            ->addOption(
                'country-info',
                'ci-file',
                InputOption::VALUE_OPTIONAL,
                "Country info file",
                'countryInfo.txt'
            )
            ->addOption(
                'archive',
                'a',
                InputOption::VALUE_OPTIONAL,
                "Archive to GeoNames",
                'allCountries.zip'
            )
            ->addOption(
                'timezones-file',
                'tz-file',
                InputOption::VALUE_OPTIONAL,
                "Timezones file",
                'timeZones.txt'
            )
            ->addOption(
                'admin1-codes',
                'a1',
                InputOption::VALUE_OPTIONAL,
                "Admin 1 Codes file",
                'admin1CodesASCII.txt'
            )
            ->addOption(
                'hierarchy',
                'hi',
                InputOption::VALUE_OPTIONAL,
                "Hierarchy ZIP file",
                'hierarchy.zip'
            )
            ->addOption(
                'admin2-codes',
                'a2',
                InputOption::VALUE_OPTIONAL,
                "Admin 2 Codes file",
                'admin2Codes.txt'
            )
            ->addOption(
                'languages-codes',
                'lc',
                InputOption::VALUE_OPTIONAL,
                "Admin 2 Codes file",
                'iso-languagecodes.txt'
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
                InputOption::VALUE_OPTIONAL,
                '',
                false)
            ->addOption(
                "skip-geoname",
                null,
                InputOption::VALUE_OPTIONAL,
                '',
                false)
            ->addOption(
                "skip-hierarchy",
                null,
                InputOption::VALUE_OPTIONAL,
                '',
                false
            )
            ;
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->endpoint = $input->getOption('endpoint');

        $downloadDir = $input->getOption('download-dir') ?:
            $this->bag->get("kernel.cache_dir") . DIRECTORY_SEPARATOR . 'geoname';

        !file_exists($downloadDir) && mkdir($downloadDir, 0700, true);

        $downloadDir = realpath($downloadDir);


        //timezones
        if ($input->getOption('timezones')) {
            $timezones = $input->getOption('timezones');
            $timezonesLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($timezones);

            $this->downloadWithProgressBar(
                $timezones,
                $timezonesLocal,
                $output
            );
        }


        // country-info
        if ($input->getOption('countries')) {
            $countryInfo = $input->getOption('country-info');
            $countryInfoLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($countryInfo);

            $this->downloadWithProgressBar(
                $countryInfo,
                $countryInfoLocal,
                $output
            );

            $this->importWithProgressBar(
                $this->countryImport,
                $countryInfoLocal,
                "Importing Countries",
                $output
            );
        }

//        return self::SUCCESS;

        //importing

        $output->writeln('');

//        $this->importWithProgressBar(
//            $this->getContainer()->get("bordeux.geoname.import.timezone"),
//            $timezonesLocal,
//            "Importing timezones",
//            $output
//        );
//
//        $output->writeln('');

        if (!$input->getOption("skip-admin1")) {
            $output->writeln('admin1');
            // admin1
            $admin1 = $input->getOption('admin1-codes');
            $admin1Local = $downloadDir . DIRECTORY_SEPARATOR . basename($admin1);

            $this->downloadWithProgressBar(
                $admin1,
                $admin1Local,
                $output
            );

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.administrative"),
                $admin1Local,
                "Importing administrative 1",
                $output
            );

            $output->writeln('');
        }


        if (!$input->getOption("skip-admin2")) {
            $admin2 = $input->getOption('admin2-codes');
            $admin2Local = $downloadDir . DIRECTORY_SEPARATOR . basename($admin2);


            $this->downloadWithProgressBar(
                $admin2,
                $admin2Local,
                $output
            );
            $output->writeln('');

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.administrative"),
                $admin2Local,
                "Importing administrative 2",
                $output
            );


            $output->writeln('');
        }


        if (!$input->getOption("skip-geoname")) {
            // archive
            $archive = $input->getOption('archive');
            $archiveLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($archive);

            $this->downloadWithProgressBar(
                $archive,
                $archiveLocal,
                $output
            );
            $output->writeln('');

            $this->importWithProgressBar(
                $this->getContainer()->get("bordeux.geoname.import.geoname"),
                $archiveLocal,
                "Importing GeoNames",
                $output,
                1000
            );


            $output->writeln("");
        }


        if (0)
        if (!$input->getOption("skip-hierarchy")) {
            // archive
            $archive = $input->getOption('hierarchy');
            $archiveLocal = $downloadDir . DIRECTORY_SEPARATOR . basename($archive);

            $this->downloadWithProgressBar(
                $archive,
                $archiveLocal,
                $output
            );
            $output->writeln('');

            $this->importWithProgressBar(
                $this->hierarchyImport,
                $archiveLocal,
                "Importing Hierarchy",
                $output,
                1000
            );


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
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function importWithProgressBar(ImportInterface $importer, $file, $message, OutputInterface $output, $steps = 100): bool
    {
        $progress = new ProgressBar($output, $steps);
        $progress->setFormat(self::PROGRESS_FORMAT);
        $progress->setMessage($message);
        $progress->setRedrawFrequency(1);
        $progress->start();

        if ($result = $importer->import(
            $file,
            function ($percent) use ($progress, $steps) {
                $progress->setProgress((int)($percent * $steps));
            }
        )) {
            $progress->finish();
        }

        return $result;
    }


    /**
     * @param string $url
     * @param string $saveAs
     * @param OutputInterface $output
     * @return \GuzzleHttp\Promise\PromiseInterface
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function downloadWithProgressBar($filename, $saveAs, OutputInterface $output)
    {
<<<<<<< HEAD
        $url =  $this->endpoint . $filename;
        if (file_exists($saveAs)) {
            $output->writeln(pathinfo($saveAs, PATHINFO_FILENAME) . " exists in the cache.");
        } else {
            $progress = new ProgressBar($output, 100);
            $progress->setFormat(self::PROGRESS_FORMAT);
            $progress->setMessage("Start downloading {$url}");
            $progress->setRedrawFrequency(1);
            $progress->start();

            $this->download(
                $url,
                $saveAs,
                function ($percent) use ($progress) {
                    $progress->setProgress((int)($percent * 100));
                }
            );
=======
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
>>>>>>> symfony5
            $progress->finish();
        }


    }


    /**
     * @param string $url
     * @param string $output
     * @param callable $progress
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function download($url, $saveAs, callable $progress)
    {
        $client = HttpClient::create();
        $response = $client->request('GET', $url, [
            'on_progress' => function (int $downloadedSize, int $totalSize, array $info) use ($progress): void {
                $totalSize && is_callable($progress) && $progress($downloadedSize / $totalSize);
            },
        ]);

        $fileHandler = fopen($saveAs, 'w');
        foreach ($client->stream($response) as $chunk) {
            fwrite($fileHandler, $chunk->getContent());
        }
    }
}
