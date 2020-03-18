package server

// stream is a message stream consisting of one or more partitions. Each
// partition maps to a NATS subject and is the unit of replication.
type stream struct {
	name       string
	subject    string
	partitions map[int32]*partition
}

// Close the stream by closing each of its partitions.
func (p *stream) Close() error {
	for _, partition := range p.partitions {
		if err := partition.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Pause the stream by pausing each of its partitions.
func (p *stream) Pause() error {
	for _, partition := range p.partitions {
		if err := partition.Pause(); err != nil {
			return err
		}
	}
	return nil
}

// Delete the stream by closing and deleting each of its partitions.
func (p *stream) Delete() error {
	for _, partition := range p.partitions {
		if err := partition.Delete(); err != nil {
			return err
		}
	}
	return nil
}
