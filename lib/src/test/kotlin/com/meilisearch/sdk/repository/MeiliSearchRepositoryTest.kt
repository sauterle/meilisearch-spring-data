package com.meilisearch.sdk.repository

import com.google.common.collect.Lists
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.test.util.AssertionErrors
import org.springframework.test.util.ReflectionTestUtils
import java.util.*

data class DummyEntity(val id: String, var field: String)

internal class MeiliSearchRepositoryTest {

    companion object{
        const val WRONG_ID = "wrong_id"
    }

    private val meiliSearchUrl = "http://localhost:7700"

    private val privateKey = "<TODO>"

    private val entity = DummyEntity(UUID.randomUUID().toString(), "a value")

    private val meiliSearchRepository: MeiliSearchRepository<DummyEntity, String> =
        object: MeiliSearchRepository<DummyEntity, String>(ObjectMapperTestConfig().objectMapper(), meiliSearchUrl, privateKey, true, "unit-test"){}

    @BeforeEach
    fun setUp() {
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
        assertFalse(Lists.newArrayList(all).isEmpty())
        assertEntityIsExpectedType(Lists.newArrayList(all).stream().findFirst().orElseThrow())
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
        val dummyEntities = Lists.newArrayList(all)
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
        assertFalse(Lists.newArrayList(found).isEmpty())
        assertEquals(1, Lists.newArrayList(found).size)
        assertEntityIsExpectedType(Lists.newArrayList(found).stream().findAny().orElseThrow())
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