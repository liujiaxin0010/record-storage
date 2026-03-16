package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"recording-server/pb/filer_pb"
)

type GRPCFilerClient struct {
	conn   *grpc.ClientConn
	client filer_pb.SeaweedFilerClient
}

func NewGRPCFilerClient(addrs []string) (*GRPCFilerClient, error) {
	var lastErr error
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err == nil {
			return &GRPCFilerClient{conn: conn, client: filer_pb.NewSeaweedFilerClient(conn)}, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no filer grpc address configured")
	}
	return nil, lastErr
}

func (c *GRPCFilerClient) CreateEntry(ctx context.Context, meta EntryMeta) error {
	dir, name := splitObjectKey(meta.Key)
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := c.client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name: name,
			Attributes: &filer_pb.FuseAttributes{
				FileSize: uint64(meta.Size),
				Mtime:    meta.ModifiedAt / int64(time.Second),
				Crtime:   meta.CreatedAt / int64(time.Second),
				Mime:     meta.MimeType,
			},
			Chunks: []*filer_pb.FileChunk{{
				FileId:       meta.FileID,
				Offset:       0,
				Size:         uint64(meta.Size),
				ModifiedTsNs: meta.ModifiedAt,
				ETag:         meta.ETag,
			}},
			Extended: map[string][]byte{
				"volume_id": []byte(meta.VolumeID),
			},
		},
		SkipCheckParentDirectory: true,
	})
	if err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

func (c *GRPCFilerClient) LookupEntry(ctx context.Context, key string) (EntryMeta, error) {
	dir, name := splitObjectKey(key)
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	resp, err := c.client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return EntryMeta{}, classifyGRPCError(err)
	}
	entry := resp.GetEntry()
	if entry == nil {
		return EntryMeta{}, ErrNotFound
	}
	meta := EntryMeta{Key: key}
	if attributes := entry.GetAttributes(); attributes != nil {
		meta.Size = int64(attributes.GetFileSize())
		meta.MimeType = attributes.GetMime()
		meta.CreatedAt = attributes.GetCrtime() * int64(time.Second)
		meta.ModifiedAt = attributes.GetMtime() * int64(time.Second)
	}
	if chunks := entry.GetChunks(); len(chunks) > 0 {
		meta.FileID = chunks[0].GetFileId()
		meta.ETag = chunks[0].GetETag()
		if meta.Size == 0 {
			meta.Size = int64(chunks[0].GetSize())
		}
	}
	if extended := entry.GetExtended(); extended != nil {
		meta.VolumeID = string(extended["volume_id"])
	}
	if meta.VolumeID == "" {
		meta.VolumeID = parseVolumeID(meta.FileID)
	}
	return meta, nil
}

func (c *GRPCFilerClient) DeleteEntry(ctx context.Context, key string, recursive bool) error {
	dir, name := splitObjectKey(key)
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := c.client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:    dir,
		Name:         name,
		IsDeleteData: true,
		IsRecursive:  recursive,
	})
	if err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

func (c *GRPCFilerClient) ListEntries(ctx context.Context, prefix string, limit int, startFrom string) ([]EntryMeta, bool, error) {
	dir, startName := splitObjectKey(strings.TrimSuffix(prefix, "/") + "/")
	if startFrom != "" {
		startName = path.Base(startFrom)
	}
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	stream, err := c.client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
		Directory:         strings.TrimSuffix(dir, "/"),
		Prefix:            path.Base(strings.TrimSuffix(prefix, "/")),
		StartFromFileName: startName,
		Limit:             uint32(limit),
	})
	if err != nil {
		return nil, false, classifyGRPCError(err)
	}
	var result []EntryMeta
	for {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, false, classifyGRPCError(recvErr)
		}
		entry := resp.GetEntry()
		if entry == nil {
			continue
		}
		key := path.Join(strings.TrimSuffix(dir, "/"), entry.GetName())
		meta, err := entryToMeta(key, entry)
		if err != nil {
			return nil, false, err
		}
		result = append(result, meta)
	}
	return result, len(result) >= limit && limit > 0, nil
}

func (c *GRPCFilerClient) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := c.client.Ping(ctx, &filer_pb.PingRequest{})
	if err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

func (c *GRPCFilerClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func splitObjectKey(key string) (string, string) {
	clean := "/" + strings.TrimPrefix(strings.TrimSpace(key), "/")
	return path.Dir(clean), path.Base(clean)
}

func entryToMeta(key string, entry *filer_pb.Entry) (EntryMeta, error) {
	meta := EntryMeta{Key: key}
	if entry == nil {
		return meta, ErrNotFound
	}
	if attributes := entry.GetAttributes(); attributes != nil {
		meta.Size = int64(attributes.GetFileSize())
		meta.MimeType = attributes.GetMime()
		meta.CreatedAt = attributes.GetCrtime() * int64(time.Second)
		meta.ModifiedAt = attributes.GetMtime() * int64(time.Second)
	}
	if chunks := entry.GetChunks(); len(chunks) > 0 {
		meta.FileID = chunks[0].GetFileId()
		meta.ETag = chunks[0].GetETag()
	}
	if extended := entry.GetExtended(); extended != nil {
		meta.VolumeID = string(extended["volume_id"])
	}
	if meta.VolumeID == "" {
		meta.VolumeID = parseVolumeID(meta.FileID)
	}
	return meta, nil
}
