package com.meilisearch.sdk.repository

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.meilisearch.sdk.*
import org.slf4j.LoggerFactory
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.*

data class UpdateResponse(val updateId: Int)

abstract class MeiliSearchRepository<T, ID>(
    private val objectMapper: ObjectMapper,
    meiliSearchUrl: String,
    privateKey: String,
    private val synchronous: Boolean,
    index: String
) : SearchDbRepository<T, ID> {
    companion object {
        const val PRIMARY_KEY = "id"
        const val LIMITS = 100000
    }

    private val log = LoggerFactory.getLogger(javaClass)
    private val client: Client = Client(Config(meiliSearchUrl, privateKey))
    protected val index = getOrCreateIndex(index)

    private fun getOrCreateIndex(indexUid: String): Index {
        return try {
            client.indexList.find { it.uid == indexUid } ?: createIndex(indexUid)
        } catch (e: java.lang.Exception) {
            throw IllegalStateException(e)
        }
    }

    private fun createIndex(indexUid: String): Index {
        val createIndexTask: Task = client.createIndex(indexUid, PRIMARY_KEY)
        client.waitForTask(createIndexTask.uid)
        return client.getIndex(indexUid)
    }

    override fun <S : T> save(entity: S): S {
        return saveAll(mutableListOf(entity)).iterator().next()
    }

    override fun <S : T> saveAll(entities: MutableIterable<S>): MutableIterable<S> {
        val documents: String = serialize<S>(entities)
        save(documents)
        return entities
    }

    private fun <S> serialize(entities: Iterable<S>): String {
        return try {
            objectMapper.writeValueAsString(entities)
        } catch (e: JsonProcessingException) {
            throw IllegalArgumentException(e)
        }
    }

    private fun save(documents: String) {
        try {
            val updateTask: Task = index.updateDocuments(documents, PRIMARY_KEY)
            if (synchronous) {
                waitAndAnalyze(updateTask)
            }
        } catch (e: Exception) {
            throw IllegalStateException(e)
        }
    }

    private fun waitAndAnalyze(task: Task) {
        try {
            client.tasksHandler.waitForTask(task.uid, 180000, 500);
            val task = client.getTask(task.uid)
            if (task.error != null) {
                throw IllegalStateException("error code: '${task.error.taskErrorCode}' - error type: '${task.error.taskErrorType}' - error link: '${task.error.taskErrorLink}'")
            }
        } catch (e: Exception) {
            throw IllegalStateException(e)
        }
    }

    @Throws(JsonProcessingException::class)
    fun deserializeUpdateResponse(updateResponse: String?): UpdateResponse {
        return objectMapper.readValue(updateResponse, UpdateResponse::class.java)
    }

    override fun findById(id: ID): Optional<T> {
        return try {
            val document = index.getDocument(id.toString())
            Optional.ofNullable(deserializeOne(document))
        } catch (e: java.lang.Exception) {
            log.warn("Could not find by id...", e)
            Optional.empty()
        }
    }

    open fun deserializeOne(savedDocuments: String): T {
        return try {
            val type: JavaType = objectMapper.typeFactory.constructType(getGenericType())
            objectMapper.readValue(savedDocuments, type)
        } catch (e: JsonProcessingException) {
            throw IllegalArgumentException(e)
        }
    }

    private fun getGenericType(): Type {
        val genericSuperclass = this.javaClass.genericSuperclass as ParameterizedType
        return genericSuperclass.actualTypeArguments[0]
    }

    override fun existsById(id: ID): Boolean {
        return try {
            val document = index.getDocument(id.toString())
            Optional.ofNullable(deserializeOne(document)).isPresent
        } catch (e: java.lang.Exception) {
            false
        }
    }

    override fun findAll(): List<T> {
        val documents: String = findAllDocuments()
        return deserialize(documents)
    }

    private fun findAllDocuments(): String {
        return try {
            index.getDocuments(LIMITS)
        } catch (e: java.lang.Exception) {
            throw IllegalStateException(e)
        }
    }

    private fun deserialize(savedDocuments: String): List<T> {
        return try {
            val type: JavaType = objectMapper.typeFactory.constructCollectionType(
                MutableList::class.java, getGenericClass()
            )
            objectMapper.readValue(savedDocuments, type)
        } catch (e: JsonProcessingException) {
            throw IllegalArgumentException(e)
        }
    }

    private fun getGenericClass(): Class<*>? {
        return try {
            val actualTypeArguments = (javaClass.genericSuperclass as ParameterizedType)
                .actualTypeArguments
            val typeName = actualTypeArguments[0].typeName
            Class.forName(typeName)
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException(e)
        }
    }

    override fun findAllById(ids: MutableIterable<ID>): MutableIterable<T> {
        return ids.map { findById(it) }.filter { it.isPresent }
            .map { it.get() }.toMutableList()
    }

    override fun count(): Long {
        return try {
            val searchRequest = SearchRequest()
            val search = index.search(searchRequest)
            search.nbHits.toLong()
        } catch (e: java.lang.Exception) {
            throw IllegalStateException(e)
        }
    }

    override fun deleteById(id: ID) {
        try {
            val deleteTask: Task = index.deleteDocument(id.toString())
            if (synchronous) {
                waitAndAnalyze(deleteTask)
            }
        } catch (e: java.lang.Exception) {
            throw IllegalStateException(e)
        }
    }

    override fun delete(entity: T) {
        throw UnsupportedOperationException("Not yet Implemented")
    }

    override fun deleteAllById(ids: MutableIterable<ID>) {
        throw UnsupportedOperationException("Not yet Implemented")
    }

    override fun deleteAll(entities: MutableIterable<T>) {
        throw UnsupportedOperationException("Not yet Implemented")
    }

    override fun deleteAll() {
        try {
            val deleteTask: Task = index.deleteAllDocuments()
            if (synchronous) {
                waitAndAnalyze(deleteTask)
            }
        } catch (e: Exception) {
            throw IllegalStateException(e)
        }
    }
}