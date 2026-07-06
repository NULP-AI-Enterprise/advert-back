package com.advertising.repository;

import com.advertising.model.entity.MediaItem;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Repository
public interface MediaRepository extends JpaRepository<MediaItem, UUID> {

    // Returns [CAST(id AS text), similarity::double] — entity loaded separately to avoid native-query Object[] casting issues
    @Query(value = """
        SELECT CAST(id AS text), 1 - (embedding <=> CAST(:embedding AS vector)) AS similarity
        FROM media_items
        WHERE embedding IS NOT NULL
          AND 1 - (embedding <=> CAST(:embedding AS vector)) >= :threshold
        ORDER BY embedding <=> CAST(:embedding AS vector)
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findSimilarByEmbedding(
        @Param("embedding") String embeddingJson,
        @Param("threshold") double threshold,
        @Param("limit") int limit
    );

    @Query(value = """
        SELECT CAST(id AS text), 0.5 AS similarity
        FROM media_items
        WHERE LOWER(title)       LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(description) LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(category)    LIKE LOWER(CONCAT('%', :query, '%'))
           OR EXISTS (
               SELECT 1 FROM unnest(tags) t
               WHERE LOWER(t) LIKE LOWER(CONCAT('%', :query, '%'))
           )
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByTextFallback(
        @Param("query") String query,
        @Param("limit") int limit
    );

    @Query(value = "SELECT CAST(id AS text), 0.3 AS similarity FROM media_items ORDER BY created_at DESC LIMIT :limit",
           nativeQuery = true)
    List<Object[]> findTopN(@Param("limit") int limit);

    @Query(value = """
        SELECT CAST(id AS text), 0.5 AS similarity
        FROM media_items
        WHERE (
              :query = ''
           OR LOWER(title)       LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(description) LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(category)    LIKE LOWER(CONCAT('%', :query, '%'))
           OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE LOWER(t) LIKE LOWER(CONCAT('%', :query, '%')))
        )
        AND (
              :region = ''
           OR LOWER(CAST(audience AS text)) LIKE LOWER(CONCAT('%', :region, '%'))
           OR LOWER(CAST(metrics AS text))  LIKE LOWER(CONCAT('%', :region, '%'))
           OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE LOWER(t) LIKE LOWER(CONCAT('%', :region, '%')))
        )
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByTextAndRegion(
        @Param("query") String query,
        @Param("region") String region,
        @Param("limit") int limit
    );

    @Query(value = """
        SELECT CAST(id AS text), 0.35 AS similarity
        FROM media_items
        WHERE metrics->>'reach_tier' = :reachTier
        ORDER BY created_at DESC
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByReachTier(@Param("reachTier") String reachTier, @Param("limit") int limit);

    @Modifying
    @Transactional
    @Query(value = "UPDATE media_items SET embedding = CAST(:embedding AS vector), updated_at = NOW() WHERE id = CAST(:id AS uuid)",
           nativeQuery = true)
    void updateEmbeddingById(@Param("id") String id, @Param("embedding") String embedding);

    @Query("""
        SELECT m FROM MediaItem m
        WHERE (:search = '' OR LOWER(m.title) LIKE LOWER(CONCAT('%', :search, '%')))
          AND (:category = '' OR m.category = :category)
        """)
    Page<MediaItem> findByFilters(
        @Param("search") String search,
        @Param("category") String category,
        Pageable pageable
    );
}
