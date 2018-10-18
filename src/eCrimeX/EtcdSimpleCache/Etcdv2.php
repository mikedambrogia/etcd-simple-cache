<?php
namespace eCrimeX\EtcdSimpleCache;


use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Psr\SimpleCache\CacheInterface;
use \DateInterval;

class Etcdv2 implements CacheInterface
{
    public $key_base_uri = '/v2/keys/';

    public $key_root_prefix = '';

    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $etcd_host;

    /**
     * @var int
     */
    private $etcd_port;

    /**
     * @var string
     */
    private $etcd_protocol;

    /**
     * @return string
     */
    public function getKeyBaseUri()
    {
        return $this->key_base_uri;
    }

    /**
     * @param string $key_base_uri
     * @return Etcdv2
     */
    public function setKeyBaseUri($key_base_uri)
    {
        $this->key_base_uri = $key_base_uri;
        return $this;
    }

    /**
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param Client $client
     * @return Etcdv2
     */
    public function setClient($client)
    {
        $this->client = $client;
        return $this;
    }

    /**
     * @return string
     */
    public function getEtcdHost()
    {
        return $this->etcd_host;
    }

    /**
     * @param string $etcd_host
     * @return Etcdv2
     */
    public function setEtcdHost($etcd_host)
    {
        $this->etcd_host = $etcd_host;
        return $this;
    }

    /**
     * @return string
     */
    public function getKeyRootPrefix()
    {
        return $this->key_root_prefix;
    }

    /**
     * @param string $key_root_prefix
     * @return Etcdv2
     */
    public function setKeyRootPrefix($key_root_prefix)
    {
        $this->key_root_prefix = $key_root_prefix;
        return $this;
    }

    /**
     * @return int
     */
    public function getEtcdPort()
    {
        return $this->etcd_port;
    }

    /**
     * @param int $etcd_port
     * @return Etcdv2
     */
    public function setEtcdPort($etcd_port)
    {
        $this->etcd_port = $etcd_port;
        return $this;
    }

    /**
     * @return string
     */
    public function getEtcdProtocol()
    {
        return $this->etcd_protocol;
    }

    /**
     * @param string $etcd_protocol
     * @return Etcdv2
     */
    public function setEtcdProtocol($etcd_protocol)
    {
        $this->etcd_protocol = $etcd_protocol;
        return $this;
    }

    public function __construct(
        Client $client,
        $etcd_protocol = 'http',
        $etcd_host = '127.0.0.1',
        $etcd_port = 2379
    )
    {
        $this->client = $client;
        $this->etcd_port = $etcd_port;
        $this->etcd_host = $etcd_host;
        $this->etcd_protocol = $etcd_protocol;
    }

    /**
     * Wipes clean the entire cache's keys.
     *
     * @return bool True on success and false on failure.
     */
    public function clear()
    {
        //todo: no way to blanket delete without iterating at least the top level dir/keys and recursively deleting
        return false;
    }

    /**
     * Obtains multiple cache items by their unique keys.
     *
     * @param iterable $keys A list of keys that can obtained in a single operation.
     * @param mixed $default Default value to return for keys that do not exist.
     *
     * @return iterable A list of key => value pairs. Cache keys that do not exist or are stale will have $default as value.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $keys is neither an array nor a Traversable,
     *   or if any of the $keys are not a legal value.
     * @throws \Exception
     */
    public function getMultiple($keys, $default = null)
    {
        $ret = [];
        foreach ($keys as $key) {
            $ret[$key] = $this->get($key, $default);
        }
        return $ret;
    }

    /**
     * Fetches a value from the cache.
     *
     * @param string $key The unique key of this item in the cache.
     * @param mixed $default Default value to return if the key does not exist.
     *
     * @return mixed The value of the item from the cache, or $default in case of cache miss.
     *
     * @throws \Exception
     */
    public function get($key, $default = null)
    {
        if (!$this->has($key)) {
            return $default;
        }
        $key_p = $this->keyToPath($key) . '/v';
        $resp = $this->client->get($this->buildKeyURL($key_p));
        $res = json_decode($resp->getBody()->getContents(), true);
        return $res['node']['value'];
    }

    /**
     * Determines whether an item is present in the cache.
     *
     * NOTE: It is recommended that has() is only to be used for cache warming type purposes
     * and not to be used within your live applications operations for get/set, as this method
     * is subject to a race condition where your has() will return true and immediately after,
     * another script can remove it making the state of your app out of date.
     *
     * @param string $key The cache item key.
     *
     * @return bool
     *
     * @throws \Exception
     */
    public function has($key)
    {
        $key_p = $this->keyToPath($key) . '/v';
        try {
            $resp = $this->client->head($this->buildKeyURL($key_p));
            if ($resp->getStatusCode() == 200) {
                return true;
            }
        } catch (ClientException $exception) {
            if ($exception->getResponse()->getStatusCode() == 404) {
                return false;
            }
            throw $exception;
        }
        return false;
    }

    private function buildKeyURL($key_p)
    {
        return $this->getEtcdProtocol() . '://' . $this->getEtcdHost() . ':' . $this->getEtcdPort() . '/' . trim($this->getKeyBaseUri(), '/') . '/' . trim($this->getKeyRootPrefix(), '/') . '/' . trim($key_p, '/');
    }

    /**
     * Converts the arbitrary key to a '/' delimited string (split on 2 chars) of the sha1 hash of the passed key
     *
     * @param $key
     * @return string
     */
    public function keyToPath($key)
    {
        return implode('/', str_split(sha1($key), 2));
    }

    /**
     * Persists a set of key => value pairs in the cache, with an optional TTL.
     *
     * @param iterable $values A list of key => value pairs for a multiple-set operation.
     * @param null|int|DateInterval $ttl Optional. The TTL value of this item. If no value is sent and
     *                                      the driver supports TTL then the library may set a default value
     *                                      for it or let the driver take care of that.
     *
     * @return bool True on success and false on failure.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $values is neither an array nor a Traversable,
     *   or if any of the $values are not a legal value.
     */
    public function setMultiple($values, $ttl = null)
    {
        foreach ($values as $key => $value) {
            $r[$key] = $this->set($key, $value, $ttl);
        }
        return true;
    }

    /**
     * Persists data in the cache, uniquely referenced by a key with an optional expiration TTL time.
     *
     * @param string $key The key of the item to store.
     * @param mixed $value The value of the item to store, must be serializable.
     * @param null|int|DateInterval $ttl Optional. The TTL value of this item. If no value is sent and
     *                                     the driver supports TTL then the library may set a default value
     *                                     for it or let the driver take care of that.
     *
     * @return bool True on success and false on failure.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function set($key, $value, $ttl = null)
    {
        $key_p = $this->keyToPath($key) . '/v';
        $dat = ['value' => $value];
        if (!is_null($ttl)) {
            $dat['ttl'] = $ttl;
        }
        try {
            $resp = $this->client->put($this->buildKeyURL($key_p), [
                'form_params' => $dat
            ]);
            return true;
        } catch (ClientException $exception) {
            return false;
        }
    }

    /**
     * Deletes multiple cache items in a single operation.
     *
     * @param iterable $keys A list of string-based keys to be deleted.
     *
     * @return bool True if the items were successfully removed. False if there was an error.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if $keys is neither an array nor a Traversable,
     *   or if any of the $keys are not a legal value.
     */
    public function deleteMultiple($keys)
    {
        foreach ($keys as $key) {
            $this->delete($key);
        }
        return true;
    }

    /**
     * Delete an item from the cache by its unique key.
     *
     * @param string $key The unique cache key of the item to delete.
     *
     * @return bool True if the item was successfully removed. False if there was an error.
     *
     * @throws \Psr\SimpleCache\InvalidArgumentException
     *   MUST be thrown if the $key string is not a legal value.
     */
    public function delete($key)
    {
        $key_p = $this->keyToPath($key) . '/v';
        try {
            $resp = $this->client->delete($this->buildKeyURL($key_p));
            return true;
        } catch (ClientException $exception) {
            return false;
        }
    }
}