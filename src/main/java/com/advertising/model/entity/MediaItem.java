package com.advertising.model.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "media_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MediaItem {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // ── Core identity ────────────────────────────────────────────────────────

    @Column(nullable = false, length = 500)
    private String title;

    @Column(length = 500)
    private String url;

    @Column(name = "marketplace_url", length = 500)
    private String marketplaceUrl;

    @Column(length = 100)
    private String country;

    // ── Placement specs (from PRNEW CSV) ─────────────────────────────────────

    @Column(name = "cost_usd", precision = 10, scale = 2)
    private BigDecimal costUsd;

    @Column(name = "format_type", length = 50)
    private String formatType;

    @Column(length = 100)
    private String language;

    @Column(name = "lead_time_hours")
    private Integer leadTimeHours;

    @Column(name = "hyperlinks_type", length = 50)
    private String hyperlinksType;

    // ── Traffic & SEO metrics (from PRNEW CSV) ────────────────────────────────

    @Column(name = "similarweb_visits")
    private Long similarwebVisits;

    @Column(name = "ahrefs_dr")
    private Integer ahrefsDr;

    @Column(name = "moz_da")
    private Integer mozDa;

    @Column(name = "semrush_score")
    private Integer semrushScore;

    @Column(name = "organic_traffic_ahrefs")
    private Integer organicTrafficAhrefs;

    @Column(name = "organic_traffic_semrush")
    private Integer organicTrafficSemrush;

    // ── Content restrictions ({crypto: true, gambling: false, ...}) ───────────

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> restrictions;

    // ── LLM-generated fields (populated by enricher) ─────────────────────────

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(length = 100)
    private String category;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Array(length = 50)
    @Column(name = "tags", columnDefinition = "text[]")
    private String[] tags;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> audience;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> metrics;

    // Hibernate cannot read the vector type — managed only via native SQL
    @Transient
    @JsonIgnore
    private float[] embedding;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
