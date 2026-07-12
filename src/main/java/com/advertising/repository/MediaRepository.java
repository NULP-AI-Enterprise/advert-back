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

    /** Same as findSimilarByEmbedding but pre-filters by format_type when non-empty. */
    @Query(value = """
        SELECT CAST(id AS text), 1 - (embedding <=> CAST(:embedding AS vector)) AS similarity
        FROM media_items
        WHERE embedding IS NOT NULL
          AND 1 - (embedding <=> CAST(:embedding AS vector)) >= :threshold
          AND (:format = '' OR LOWER(format_type) = LOWER(:format))
        ORDER BY embedding <=> CAST(:embedding AS vector)
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findSimilarByEmbeddingFiltered(
        @Param("embedding") String embeddingJson,
        @Param("threshold") double threshold,
        @Param("limit") int limit,
        @Param("format") String format
    );

    @Query(value = """
        SELECT CAST(id AS text),
               CASE WHEN :query = '' THEN 0.5
                    ELSE ts_rank(search_vector, websearch_to_tsquery('simple', :query))
               END AS similarity
        FROM media_items
        WHERE (
              :query = ''
           OR search_vector @@ websearch_to_tsquery('simple', :query)
           OR LOWER(url) LIKE LOWER(CONCAT('%', :query, '%'))
        )
        AND (
              :region = ''
           OR LOWER(country)               LIKE LOWER(CONCAT('%', :region, '%'))
           OR LOWER(CAST(audience AS text)) LIKE LOWER(CONCAT('%', :region, '%'))
           OR LOWER(CAST(metrics  AS text)) LIKE LOWER(CONCAT('%', :region, '%'))
        )
        ORDER BY COALESCE(similarweb_visits, 0) DESC,
                 ts_rank(search_vector, websearch_to_tsquery('simple', NULLIF(:query, ''))) DESC NULLS LAST
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByTextAndRegion(
        @Param("query") String query,
        @Param("region") String region,
        @Param("limit") int limit
    );

    @Query(value = """
        SELECT CAST(id AS text), 0.3 AS similarity
        FROM media_items
        ORDER BY COALESCE(similarweb_visits, 0) DESC, created_at DESC
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findTopN(@Param("limit") int limit);

    @Query(value = """
        SELECT CAST(id AS text), 0.5 AS similarity
        FROM media_items
        WHERE LOWER(title)       LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(description) LIKE LOWER(CONCAT('%', :query, '%'))
           OR LOWER(category)    LIKE LOWER(CONCAT('%', :query, '%'))
           OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE LOWER(t) LIKE LOWER(CONCAT('%', :query, '%')))
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByTextFallback(
        @Param("query") String query,
        @Param("limit") int limit
    );

    /**
     * Enriched items already classified into a canonical category — used for
     * smart padding in RecEngineService before falling back to top-traffic.
     * Ordered by traffic DESC so best outlets come first.
     */
    /** Finds outlets by exact category match OR FTS match on category term. */
    @Query(value = """
        SELECT CAST(id AS text), 0.4 AS similarity
        FROM media_items
        WHERE category = :category
           OR search_vector @@ websearch_to_tsquery('simple', :category)
        ORDER BY
            CASE WHEN category = :category THEN 1 ELSE 2 END,
            COALESCE(similarweb_visits, 0) DESC
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findByCategory(
        @Param("category") String category,
        @Param("limit") int limit
    );

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

    // ── Catalog stats — used by RecEngineService for startup diagnostics ────────

    @Query(value = "SELECT COUNT(*) FROM media_items", nativeQuery = true)
    long countAll();

    @Query(value = "SELECT COUNT(*) FROM media_items WHERE embedding IS NOT NULL", nativeQuery = true)
    long countWithEmbedding();

    @Query(value = "SELECT COUNT(*) FROM media_items WHERE description IS NOT NULL AND description != ''", nativeQuery = true)
    long countEnriched();

    @Query(value = "SELECT COUNT(DISTINCT category) FROM media_items WHERE category IS NOT NULL", nativeQuery = true)
    long countDistinctCategories();
}
