<?php


namespace Bordeux\Bundle\GeoNameBundle\Import;


use Bordeux\Bundle\GeoNameBundle\Entity\Timezone;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\ORM\EntityManager;
use GuzzleHttp\Promise\Promise;
use SplFileObject;

/**
 * Class GeoNameImport
 * @author Chris Bednarczyk <chris@tourradar.com>
 * @package Bordeux\Bundle\GeoNameBundle\Import
 */
class HierarchyImport extends GeoNameImport
{

    /**
     * @param string $filePath
     * @param callable|null $progress
     * @return bool
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function import($filePath, callable $progress = null): bool
    {

        $avrOneLineSize = 29.4;
        $batchSize = 10000;

//        if($batchSize > 1){ //temporarly
////            return true;
//        }
        $connection = $this->em->getConnection();

        $fileInside = basename($filePath, ".zip") . '.txt';
        $handler = fopen("zip://{$filePath}#{$fileInside}", 'r');
        $max = (int)filesize($filePath) / $avrOneLineSize;

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
        $connection->beginTransaction();

//        $connection->exec("START TRANSACTION");

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
                dd($csv);
                continue;
            }

            $row = array_map('trim', $csv);

            if(!isset($row[0]) || !isset($row[1])){
                dd($row);
                continue;
            }

            $geoNameId = $row[0];
            $geoNameId2 = $row[1];
            $geoNameId2 = isset($row[3]) ? $row[3] : null;
            $geoNameId2 = isset($row[4]) ? $row[4] : null;


            $query = $queryBuilder->values([
//                $geoNameId,
//                $geoNameId2
            ]);


            $buffer[] = $this->insertToReplace($query, $dbType);
            dump($query, $queryBuilder, $dbType, $buffer, $fileInside) ;
            assert(false);

            $pos++;

            if ($pos % $batchSize) {
                $this->save($buffer);
                $buffer = [];
                is_callable($progress) && $progress(($pos) / $max);
            }

        }

        !empty($buffer) &&  $this->save($buffer);;
//        $connection->exec('COMMIT');
        $connection->commit();

        return true;
    }


}
