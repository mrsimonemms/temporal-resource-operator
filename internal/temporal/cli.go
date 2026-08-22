/*
 * Copyright 2026 Simon Emms <simon@simonemms.com>
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

package temporal

import (
	"context"

	"go.temporal.io/sdk/client"
)

type Client struct {
	client client.Client
}

func (c *Client) CheckHealth(ctx context.Context) error {
	_, err := c.client.CheckHealth(ctx, nil)
	return err
}

func (c *Client) Close() {
	c.client.Close()
}

func New(ctx context.Context, opts *client.Options) (*Client, error) {
	temporalClient, err := client.DialContext(ctx, *opts)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: temporalClient,
	}, nil
}
