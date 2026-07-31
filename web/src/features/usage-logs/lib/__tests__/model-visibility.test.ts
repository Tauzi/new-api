/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import type { UsageLog } from '../../data/schema'
import { formatModelName } from '../format'

const mappedLog = {
  model_name: 'public-model',
  other: JSON.stringify({
    is_model_mapped: true,
    upstream_model_name: 'provider-secret-model',
  }),
} as UsageLog

describe('usage log model visibility', () => {
  test('hides the actual upstream model from regular users', () => {
    assert.deepEqual(formatModelName(mappedLog, false), {
      name: 'public-model',
      isMapped: false,
      actualModel: undefined,
    })
  })

  test('shows the actual upstream model to administrators', () => {
    assert.deepEqual(formatModelName(mappedLog, true), {
      name: 'public-model',
      isMapped: true,
      actualModel: 'provider-secret-model',
    })
  })
})
