<?php


namespace Bordeux\Bundle\GeoNameBundle\Import;


use Bordeux\Bundle\GeoNameBundle\Entity\Administrative;
use Bordeux\Bundle\GeoNameBundle\Entity\GeoName;
use Bordeux\Bundle\GeoNameBundle\Entity\Timezone;
use Doctrine\Common\Persistence\ManagerRegistry;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use GuzzleHttp\Promise\Promise;
use SplFileObject;

/**
 * Class AdministrativeImport
 * @author Chris Bednarczyk <chris@tourradar.com>
 * @package Bordeux\Bundle\GeoNameBundle\Import
 */
class AdministrativeImport extends GeoNameImport implements ImportInterface
{

    public function __construct(ManagerRegistry $managerRegistry)
    {
        parent::__construct($managerRegistry, Administrative::class);
    }

    /**
     * TimeZoneImport constructor.
     * @author Chris Bednarczyk <chris@tourradar.com>
     * @param EntityManager $em
     */
    public function __xxconstruct(EntityManagerInterface $em)
    {
        $this->em = $em;
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
     * @return string[]
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    public function getFieldNames()
    {
        $metaData = $this->em->getClassMetadata(Administrative::class);

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
     * @param string $filePath
     * @param callable|null $progress
     * @return bool
     * @author Chris Bednarczyk <chris@tourradar.com>
     */
    protected function _import($filePath, callable $progress = null)
    {
        $file = new SplFileObject($filePath);
        $file->setFlags(SplFileObject::READ_CSV | SplFileObject::READ_AHEAD | SplFileObject::SKIP_EMPTY | SplFileObject::DROP_NEW_LINE);
        $file->setCsvControl("\t");
        $file->seek(PHP_INT_MAX);
        $max = $file->key();
        $file->seek(1); //skip header

        $batchSize = 100;


        $administrative = $this->em->getRepository(Administrative::class);
        $administrativeTableName = $this->em
            ->getClassMetadata(Administrative::class)
            ->getTableName();

        $geoNameTableName = $this->em
            ->getClassMetadata(GeoName::class)
            ->getTableName();

        $connection = $this->em->getConnection();
        $dbType = $connection->getDatabasePlatform()->getName();

        $connection->exec( ($dbType == 'sqlite' ? 'BEGIN' : 'START') . " TRANSACTION");

        $pos = 0;

        $buffer = [];

        $queryBuilder = $connection->createQueryBuilder()
            ->insert($administrativeTableName);

        // since admins are first, insert what we know into the GeoName table, so we don't have orphans.
        $geoNameQueryBuilder = $connection->createQueryBuilder()
            ->insert($geoNameTableName);

        $fieldsNames = $this->getFieldNames();
        dump($fieldsNames);

        foreach ($file as $row) {
            $row = array_map('trim',$row);
            list(
                $code,
                $name,
                $asciiName,
                $geoNameId
                ) = $row;

            $data = [
                $fieldsNames['id'] => (int)$geoNameId,
                $fieldsNames['code'] => $this->e($code),
                $fieldsNames['name'] => $this->e($name ?: $asciiName),
                $fieldsNames['asciiName'] => $this->e($asciiName),
                // $fieldsNames['geoName'] => (int)$geoNameId,
                // $fieldsNames['geoNameId'] => (int)$geoNameId,
                ];

            $query = $queryBuilder->values($data);

            $buffer[] = $this->insertToReplace($query, $dbType);

            //
            // $data['administrative_code'] = $this->e($code);
           //    unset($data[$fieldsNames['geoNameId']]);
            unset($data[$fieldsNames['code']]);
            $data['admin_code'] = $this->e($code);
            $query = $geoNameQueryBuilder->values($data);
            $buffer[] = ($geoNameInsert = $this->insertToReplace($query, $dbType));
            // dd($buffer);

            /** @var Administrative $object */
            /* old way, very slow
            // $object = $administrative->findOneBy(['code' => $code]) ?: new Administrative();
            $object = new Administrative();
            $object->setCode($code);
            $object->setName($name);

          //  $object->setGeoName($this->em->getReference(GeoName::class, $geoNameId));
            $object->setAsciiName($asciiName);

            !$object->getId() && $this->em->persist($object);
            */

            is_callable($progress) && $progress(($pos++) / $max);
            $pos++;

            if ($pos % $batchSize) {
                $this->save($buffer);
                $buffer = [];
                is_callable($progress) && $progress(($pos) / $max);
            }

            // ???
            if($pos % 10000){
                $this->em->flush();
                $this->em->clear();
            }
        }

        !empty($buffer) && $this->save($buffer);;
        $connection->exec('COMMIT');

        return true;

        $this->em->flush();
        $this->em->clear();

        return true;
    }

}
