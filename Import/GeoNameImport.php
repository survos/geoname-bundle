<?php


namespace Bordeux\Bundle\GeoNameBundle\Import;


use Bordeux\Bundle\GeoNameBundle\Entity\Administrative;
use Bordeux\Bundle\GeoNameBundle\Entity\GeoName;
use Bordeux\Bundle\GeoNameBundle\Entity\Timezone;
use Doctrine\Common\Persistence\ManagerRegistry;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\EntityManager;
use GuzzleHttp\Promise\Promise;
use SplFileObject;

/**
 * Class GeoNameImport
 * @author Chris Bednarczyk <chris@tourradar.com>
 * @package Bordeux\Bundle\GeoNameBundle\Import
 */
class GeoNameImport implements ImportInterface
{

    private $filters;
    /**
     * @var EntityManager
     */
    public $em;

    /**
     * TimeZoneImport constructor.
     * @author Chris Bednarczyk <chris@tourradar.com>
     * @param EntityManager $em
     */

    /**
     * @param string $entityClass The class name of the entity this repository manages
     */
    public function __construct(ManagerRegistry $registry, $entityClass=GeoName::class)
    {
        $this->em = $registry->getManagerForClass($entityClass);

        if ($this->em === null) {
            throw new \LogicException(sprintf(
                'Could not find the entity manager for class "%s". Check your Doctrine configuration to make sure it is configured to load this entity’s metadata.',
                $entityClass
            ));
        }
        $this->filters = [];
    }


    public function getEntityManager() {
        return $this->em;
    }

    public function setFilters($filters) {
        $this->filters = $filters;
    }


    /**
     * @param  string $filePath
     * @param callable|null $progress
     * @return Promise|\GuzzleHttp\Promise\PromiseInterface
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function import($filePath, callable $progress = null)
    {
        $self = $this;
        /** @var Promise $promise */
        $promise = (new Promise(function () use ($filePath, $progress, $self, &$promise) {
            $promise->resolve(
                $self->_import($filePath, $progress)
            );
        }));

        return $promise;
    }

    /**
     * @param string $filePath
     * @param callable|null $progress
     * @return bool
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    protected function _import($filePath, callable $progress = null)
    {

        $avrOneLineSize = 29.4;
        $batchSize = 10000;

        $connection = $this->em->getConnection();

        $fileInside = basename($filePath, ".zip") . '.txt';
        $handler = fopen("zip://{$filePath}#{$fileInside}", 'r');
        $max = (int) (filesize($filePath) / $avrOneLineSize);

        $fieldsNames = $this->getFieldNames();

        $geoNameTableName = $this->em
            ->getClassMetadata("BordeuxGeoNameBundle:GeoName")
            ->getTableName();

        $timezoneTableName = $this->em
            ->getClassMetadata("BordeuxGeoNameBundle:Timezone")
            ->getTableName();

        $administrativeTableName = $this->em
            ->getClassMetadata("BordeuxGeoNameBundle:Administrative")
            ->getTableName();


        $dbType = $connection->getDatabasePlatform()->getName();

        // get the IDs from the admin table
        $q = $this->em->getRepository(Administrative::class)
            ->createQueryBuilder('a')
            ->select(['a.id', 'a.code'])
            ->getQuery()
            ->getResult();
        $codesById = [];
        foreach ($q as $x) {
            // $idsByCode[$x->code] = $x->id;
            $codesById[$x['id']] = $x['code'];
        }
        $administrativeGeonameIds = array_keys($codesById);
        unset($q);


        $connection->exec( ($dbType == 'sqlite' ? 'BEGIN' : 'START') . " TRANSACTION");

        $pos = 0;

        $buffer = [];

        $queryBuilder = $connection->createQueryBuilder()
            ->insert($geoNameTableName);

        while (!feof($handler)) {
            $csv = fgetcsv($handler, null, "\t");
            if (!is_array($csv)) {
                continue;
            }
            if (!isset($csv[0]) || !is_numeric($csv[0])) {
                continue;
            }

            $row = array_map('trim', $csv);

            list(
                $geoNameId,
                $name,
                $asciiName,
                $alternateNames,
                $latitude,
                $longitude,
                $featureClass,
                $featureCode,
                $countryCode,
                $cc2,
                $admin1Code,
                $admin2Code,
                $admin3Code,
                $admin4Code,
                $population,
                $elevation,
                $dem,
                $timezone,
                $modificationDate
                ) = $row;


            if (!preg_match('/^\d{4}\-\d{2}-\d{2}$/', $modificationDate)) {
                continue;
            }


            $geoNameId = (int)$geoNameId;
            $data = [
                $fieldsNames['id'] => $geoNameId, //must be as first!
                $fieldsNames['name'] => $this->e($name),
                $fieldsNames['asciiName'] => $this->e($asciiName),
                $fieldsNames['latitude'] => $latitude,
                $fieldsNames['longitude'] => $longitude,
                $fieldsNames['featureClass'] => $this->e($featureClass),
                $fieldsNames['featureCode'] => $this->e($featureCode),
                $fieldsNames['countryCode'] => $this->e($countryCode),
                $fieldsNames['cc2'] => $this->e($cc2),
                $fieldsNames['population'] => $population,
                $fieldsNames['elevation'] => $this->e($elevation),
                $fieldsNames['dem'] => $dem,
                // we don't really care about this.. $fieldsNames['modificationDate'] => $this->e($modificationDate),

                // loads doctrine relationships from Administrative entity.  Could be cached.
                $fieldsNames['timezone'] => $timezone ? "(SELECT id FROM {$timezoneTableName} WHERE timezone  =  " . $this->e($timezone) . " LIMIT 1)" : 'NULL',
                $fieldsNames['admin1'] => $admin1Code ? "(SELECT id FROM {$administrativeTableName} WHERE code  =  " . $this->e("{$countryCode}.{$admin1Code}") . " LIMIT 1)" : 'NULL',
                $fieldsNames['admin2'] => $admin2Code ? "(SELECT id FROM {$administrativeTableName} WHERE code  =  " . $this->e("{$countryCode}.{$admin1Code}.{$admin2Code}") . " LIMIT 1)" : 'NULL',
                $fieldsNames['admin3'] => $admin3Code ? "(SELECT id FROM {$administrativeTableName} WHERE code  =  " . $this->e("{$countryCode}.{$admin1Code}.{$admin3Code}") . " LIMIT 1)" : 'NULL',
                $fieldsNames['admin4'] => $admin4Code ? "(SELECT id FROM {$administrativeTableName} WHERE code  =  " . $this->e("{$countryCode}.{$admin1Code}.{$admin4Code}") . " LIMIT 1)" : 'NULL',
                ];

            $accept = true;
            foreach ($this->filters as $var => $values) {
                if ($values && count($values)) {
                    switch ($var) {
                        case 'featureCode':
                            if (!in_array($featureCode, $values)) {
                                $accept = false;
                            }
                            break;
                        case 'featureClass':
                            if (!in_array($featureClass, $values)) {
                                $accept = false;
                            }
                            break;
                        default:
                            dd("Invalid filter " . $var);
                    }
                }
            }

            // always get the geoName IDS
            if (in_array($geoNameId, $administrativeGeonameIds)) {
                $accept = true;
                $data['admin_code'] = $this->e($codesById[$geoNameId]); // move over the admin code
                /*
                $query = $queryBuilder->values($data);
                $sql = $this->insertToReplace($query, $dbType);
                dd($data, $sql);
                */
            }

            if ($accept) {
                // dd($data, 'ACCEPT');
                $query = $queryBuilder->values($data);
                $buffer[] = $this->insertToReplace($query, $dbType);
            } else {
                // reject
                // dump($this->filters, $featureCode, $values, $data, 'REJECT');
            }

            $pos++;
            if ( ($pos % $batchSize) == 0) {
                if (count($buffer)) {
                    $this->save($buffer);
                    $buffer = [];
                }
                is_callable($progress) && $progress(($pos) / $max);
            }

        }

        !empty($buffer) && $this->save($buffer);;
        $connection->exec('COMMIT');

        return true;
    }


    /**
     * @param QueryBuilder $insertSQL
     * @return mixed
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function insertToReplace(QueryBuilder $insertSQL, $dbType)
    {

        if ( in_array($dbType,  ['sqlite', 'mysql'])) {
            $sql = $insertSQL->getSQL();
            return preg_replace('/' . preg_quote('INSERT ', '/') . '/', 'REPLACE ', $sql, 1);
        }

        if ($dbType == "postgresql") {
            $vals = $insertSQL->getQueryPart("values");
            $sql = $insertSQL->getSQL();
            reset($vals);
            $index = key($vals);
            array_shift($vals);

            $parts = [];
            foreach ($vals as $column => $val) {
                $parts[] = "{$column} = {$val}";
            }

            $sql .= " ON CONFLICT ({$index}) DO UPDATE  SET " . implode(", ", $parts);

            return $sql;
        }

        throw new \Exception("Unsupported database type");
    }

    /**
     * @param $queries
     * @return bool
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function save($queries)
    {
        $queries = implode("; \n", $queries);
        $this->em->getConnection()->exec($queries);

        return true;
    }


    /**
     * @return string[]
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function getFieldNames()
    {
        $metaData = $this->em->getClassMetadata("BordeuxGeoNameBundle:GeoName");

        $result = [];

        foreach ($metaData->getFieldNames() as $name) {
            $result[$name] = $metaData->getColumnName($name);
        }

        foreach ($metaData->getAssociationNames() as $name) {
            if ($metaData->isSingleValuedAssociation($name)) {
                $result[$name] = $metaData->getSingleAssociationJoinColumnName($name);
            }
        }

        return $result;
    }

    /**
     * @param string $val
     * @return string
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    protected function e($val)
    {
        if ($val === null || strlen($val) === 0) {
            return 'NULL';
        }
        return $this->em->getConnection()->quote($val);
    }

}
