<?php
/**
 * Plugin Name: JapanTravel SEO Hardening
 * Description: Hardening rules for taxonomy/archive noindex, sitemap cleanup, and robots.txt consistency.
 * Version: 0.1.0
 */

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

function japantravel_seo_hardening_is_archive_noindex_target() {
	return is_category() || is_tag() || is_author() || is_date() || is_search() || is_attachment();
}

add_filter(
	'robots_txt',
	function ( $output, $public ) {
		$site_url     = home_url();
		$sitemap_line = 'Sitemap: ' . trailingslashit( $site_url ) . 'sitemap.xml';
		if ( strpos( $output, $sitemap_line ) === false ) {
			$output = trim( $output ) . "\n\n" . $sitemap_line . "\n";
		}
		return $output;
	},
	10,
	2
);

add_filter(
	'rank_math/frontend/robots',
	function ( $robots ) {
		if ( japantravel_seo_hardening_is_archive_noindex_target() ) {
			unset( $robots['index'] );
			$robots['noindex'] = 'noindex';
			$robots['follow']  = 'follow';
		}
		return $robots;
	}
);

add_filter(
	'wp_robots',
	function ( $robots ) {
		if ( japantravel_seo_hardening_is_archive_noindex_target() ) {
			$robots['noindex'] = true;
			$robots['follow']  = true;
		}
		return $robots;
	}
);

add_action(
	'send_headers',
	function () {
		if ( is_feed() ) {
			header( 'X-Robots-Tag: noindex, follow', true );
		}
	}
);

add_filter(
	'rank_math/sitemap/exclude_taxonomy',
	function ( $exclude, $type ) {
		if ( in_array( $type, array( 'category', 'post_tag' ), true ) ) {
			return true;
		}
		return $exclude;
	},
	10,
	2
);

add_filter(
	'rank_math/sitemap/exclude_empty_terms',
	function ( $exclude, $taxonomy_names ) {
		return true;
	},
	10,
	2
);

if ( function_exists( 'remove_action' ) ) {
	remove_action( 'wp_head', 'feed_links', 2 );
	remove_action( 'wp_head', 'feed_links_extra', 3 );
}
