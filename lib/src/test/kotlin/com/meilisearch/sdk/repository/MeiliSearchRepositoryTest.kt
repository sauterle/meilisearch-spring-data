package com.meilisearch.sdk.repository

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.test.util.AssertionErrors
import org.springframework.test.util.ReflectionTestUtils
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.*

data class DummyEntity(val id: String, var field: String)

@Testcontainers
internal class MeiliSearchRepositoryTest {

    companion object {
        const val WRONG_ID = "wrong_id"
        const val API_KEY = "123"
    }

    @Container
    private var meilisearchDB: GenericContainer<*> = GenericContainer(DockerImageName.parse("getmeili/meilisearch:v1.6.2"))
        .withExposedPorts(7700).withEnv("MEILI_MASTER_KEY", API_KEY)

    private val privateKey = API_KEY

    private val entity = DummyEntity(UUID.randomUUID().toString(), "a value")

    private lateinit var meiliSearchRepository: MeiliSearchRepository<DummyEntity, String>

    @BeforeEach
    fun setUp() {
        val meiliSearchUrl = "http://${meilisearchDB.getHost()}:${meilisearchDB.getFirstMappedPort()}"

        meiliSearchRepository = object : MeiliSearchRepository<DummyEntity, String>(
            ObjectMapperTestConfig().objectMapper(),
            meiliSearchUrl,
            privateKey,
            true,
            500,
            "unit-test"
        ) {}

        ReflectionTestUtils.setField(meiliSearchRepository, "synchronous", true)
        meiliSearchRepository.save(entity)
    }

    @AfterEach
    fun tearDown() {
        meiliSearchRepository.deleteAll()
    }

    @Test
    fun testFindAll() {
        val all: Iterable<DummyEntity> = meiliSearchRepository.findAll()

        assertFalse(all.toList().isEmpty())
        assertEntityIsExpectedType(all.toList().stream().findFirst().orElseThrow())
    }

    @Test
    fun testSave() {
        meiliSearchRepository.save(entity)
    }

    @Test
    fun testUpdate() {
        meiliSearchRepository.save(entity)
        entity.field = "changed"
        meiliSearchRepository.save(entity)
        val all: Iterable<DummyEntity> = meiliSearchRepository.findAll()
        val next = all.iterator().next()
        val dummyEntities = all.toList()
        assertEquals(1, dummyEntities.size)
        assertEquals("changed", next.field)
    }

    @Test
    fun testSaveAll() {
        meiliSearchRepository.saveAll(mutableListOf(entity))
    }

    @Test
    fun testFindById() {
        val foundEntity = meiliSearchRepository.findById(entity.id)
        assertFalse(foundEntity.isEmpty)
        assertEntityIsExpectedType(foundEntity.orElseThrow())
    }

    private fun assertEntityIsExpectedType(entity: Any) {
        AssertionErrors.assertTrue(
            "Object is not a " + DummyEntity::class.java + " but rather " + entity.javaClass,
            entity is DummyEntity
        )
    }

    @Test
    fun testFindByIdNotFound() {
        val foundEntity = meiliSearchRepository.findById(WRONG_ID)
        AssertionErrors.assertTrue("should be empty", foundEntity.isEmpty)
    }

    @Test
    fun testExistsById() {
        AssertionErrors.assertTrue("should exist", meiliSearchRepository.existsById(entity.id))
    }

    @Test
    fun testExistsByIdNotFound() {
        assertFalse(meiliSearchRepository.existsById(WRONG_ID))
    }

    @Test
    fun testFindAllById() {
        val found: Iterable<DummyEntity> = meiliSearchRepository.findAllById(mutableListOf(entity.id, WRONG_ID))
        assertFalse(found.toList().isEmpty())
        assertEquals(1, found.toList().size)
        assertEntityIsExpectedType(found.toList().stream().findAny().orElseThrow())
    }

    @Test
    fun testCount() {
        val count = meiliSearchRepository.count()
        assertEquals(1, count)
    }

    @Test
    fun testDeleteById() {
        meiliSearchRepository.deleteById(entity.id)
    }

    @Test
    fun testDelete() {
        assertThrows(
            UnsupportedOperationException::class.java
        ) { meiliSearchRepository.delete(entity) }
    }

    @Test
    fun testDeleteAll() {
        meiliSearchRepository.deleteAll()
    }

    @Test
    fun testDeleteAllGivenEntities() {
        assertThrows(
            UnsupportedOperationException::class.java
        ) { meiliSearchRepository.deleteAll(mutableListOf(entity)) }
    }

}