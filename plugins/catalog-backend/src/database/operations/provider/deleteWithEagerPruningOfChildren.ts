/*
 * Copyright 2022 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Knex } from 'knex';
import lodash from 'lodash';
import { DbRefreshStateReferencesRow, DbRefreshStateRow } from '../../tables';

/**
 * Given a number of entity refs originally created by a given entity provider
 * (source key), remove those entities from the refresh state, and at the same
 * time recursively remove every child that is a direct or indirect result of
 * processing those entities, if they would have otherwise become orphaned by
 * the removal of their parents.
 */
export async function deleteWithEagerPruningOfChildren(options: {
  tx: Knex.Transaction;
  entityRefs: string[];
  sourceKey: string;
}): Promise<number> {
  const { tx, entityRefs, sourceKey } = options;

  // Split up the operation by (large) chunks, so that we do not hit database
  // limits for the number of permitted bindings on a precompiled statement
  let removedCount = 0;
  for (const refs of lodash.chunk(entityRefs, 1000)) {
    /*
    WITH RECURSIVE
      -- All the nodes that can be reached downwards from our root
      descendants(entity_ref) AS (
        SELECT target_entity_ref
        FROM refresh_state_references
        WHERE source_key = "R1" AND target_entity_ref IN [...refs]
        UNION
        SELECT target_entity_ref
        FROM descendants
        JOIN refresh_state_references ON source_entity_ref = descendants.entity_ref
      ),
      -- All the individual relations that can be reached upwards from each descendant
      ancestors(source_key, source_entity_ref, target_entity_ref, subject) AS (
        SELECT source_key, source_entity_ref, target_entity_ref, descendants.entity_ref
        FROM descendants
        JOIN refresh_state_references ON refresh_state_references.target_entity_ref = descendants.entity_ref
        UNION
        SELECT
          refresh_state_references.source_key,
          refresh_state_references.source_entity_ref,
          refresh_state_references.target_entity_ref,
          ancestors.subject
        FROM ancestors
        JOIN refresh_state_references ON refresh_state_references.target_entity_ref = ancestors.source_entity_ref
      )
    WITH
      -- Retain entities who seem to have a root relation somewhere upwards that's not part of our own deletion
      retained(entity_ref) AS (
        SELECT subject
        FROM ancestors
        WHERE source_key IS NOT NULL
        AND (source_key != "R1" OR target_entity_ref NOT IN [...refs])
      )
    -- Return all descendants minus the retained ones
    SELECT descendants.entity_ref
    FROM descendants
    LEFT OUTER JOIN retained ON retained.entity_ref = descendants.entity_ref
    WHERE retained.entity_ref IS NULL
    */
    removedCount += await tx<DbRefreshStateRow>('refresh_state')
      .whereIn('entity_ref', orphans =>
        orphans
          // All the nodes that can be reached downwards from our root
          .withRecursive('descendants', ['entity_ref'], initial =>
            initial
              .select('target_entity_ref')
              .from('refresh_state_references')
              .where('source_key', '=', sourceKey)
              .whereIn('target_entity_ref', refs)
              .union(recursive =>
                recursive
                  .select('refresh_state_references.target_entity_ref')
                  .from('descendants')
                  .join(
                    'refresh_state_references',
                    'descendants.entity_ref',
                    'refresh_state_references.source_entity_ref',
                  ),
              ),
          )
          // All the relations that can be reached upwards from each descendant
          .withRecursive(
            'ancestors',
            ['source_key', 'source_entity_ref', 'target_entity_ref', 'subject'],
            initial =>
              initial
                .select(
                  'refresh_state_references.source_key',
                  'refresh_state_references.source_entity_ref',
                  'refresh_state_references.target_entity_ref',
                  'descendants.entity_ref',
                )
                .from('descendants')
                .join('refresh_state_references', {
                  'refresh_state_references.target_entity_ref':
                    'descendants.entity_ref',
                })
                .union(recursive =>
                  recursive
                    .select(
                      'refresh_state_references.source_key',
                      'refresh_state_references.source_entity_ref',
                      'refresh_state_references.target_entity_ref',
                      'ancestors.subject',
                    )
                    .from('ancestors')
                    .join('refresh_state_references', {
                      'refresh_state_references.target_entity_ref':
                        'ancestors.source_entity_ref',
                    }),
                ),
          )
          // Retain entities who seem to have a root relation somewhere upwards that's not part of our own deletion
          .with('retained', ['entity_ref'], notPartOfDeletion =>
            notPartOfDeletion
              .select('subject')
              .from('ancestors')
              .whereNotNull('ancestors.source_key')
              .where(foreignKeyOrRef =>
                foreignKeyOrRef
                  .where('ancestors.source_key', '!=', sourceKey)
                  .orWhereNotIn('ancestors.target_entity_ref', refs),
              ),
          )
          // Return all descendants minus the retained ones
          .select('descendants.entity_ref')
          .from('descendants')
          .leftOuterJoin(
            'retained',
            'retained.entity_ref',
            'descendants.entity_ref',
          )
          .whereNull('retained.entity_ref'),
      )
      .delete();

    // Delete the references that originate only from this entity provider. Note
    // that there may be more than one entity provider making a "claim" for a
    // given root entity, if they emit with the same location key.
    await tx<DbRefreshStateReferencesRow>('refresh_state_references')
      .where('source_key', '=', sourceKey)
      .whereIn('target_entity_ref', refs)
      .delete();
  }

  return removedCount;
}
