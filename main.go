package main

import (
	"fmt"
	"strings"
)

// Constants
const (
	BlockSize  = 4096
	MaxBlocks  = 1024
	MaxKeys    = 3   // For simplicity, B-tree order is 4 (MaxKeys + 1)
	JournalMax = 100 // Maximum number of journal entries
)

// Inode structure
type Inode struct {
	InodeNumber  int
	Name         string
	IsDirectory  bool
	Size         int
	BlockPointer int
	Parent       *Inode
}

// Directory entry structure
type DirEntry struct {
	Name       string
	InodeIndex int
}

// BTreeNode structure
type BTreeNode struct {
	IsLeaf   bool
	Keys     []DirEntry
	Children []*BTreeNode
	Parent   *BTreeNode
}

// BTree structure
type BTree struct {
	Root *BTreeNode
}

// Superblock structure
type Superblock struct {
	TotalInodes int
	TotalBlocks int
	FreeBlocks  []int
	InodeMap    []*Inode
}

// Journal entry structure
type JournalEntry struct {
	Operation string
	Path      string
	Data      interface{}
}

// FileSystem structure
type FileSystem struct {
	Superblock Superblock
	DataBlocks [MaxBlocks][]byte
	Journal    []JournalEntry
}

type Snapshot struct {
	Inodes     []*Inode
	DataBlocks [MaxBlocks][]byte
}

type DirectorySnapshot struct {
	RootInode  *Inode
	Inodes     []*Inode
	DataBlocks [MaxBlocks][]byte
}

var fs FileSystem

// Initialize the filesystem
func initializeFS() {
	fs = FileSystem{
		Superblock: Superblock{
			TotalInodes: 0,
			TotalBlocks: MaxBlocks,
			FreeBlocks:  make([]int, MaxBlocks),
			InodeMap:    make([]*Inode, 0),
		},
		Journal: make([]JournalEntry, 0, JournalMax),
	}

	for i := 0; i < MaxBlocks; i++ {
		fs.Superblock.FreeBlocks[i] = i
	}

	root := createInode("root", true, nil)
	fs.Superblock.InodeMap = append(fs.Superblock.InodeMap, root)
}

// Create an inode
func createInode(name string, isDir bool, parent *Inode) *Inode {
	inode := &Inode{
		InodeNumber:  len(fs.Superblock.InodeMap),
		Name:         name,
		IsDirectory:  isDir,
		Size:         0,
		BlockPointer: allocateBlock(),
		Parent:       parent,
	}

	if isDir {
		initializeDir(inode)
	}

	fs.Superblock.TotalInodes++
	return inode
}

// Allocate a block
func allocateBlock() int {
	if len(fs.Superblock.FreeBlocks) == 0 {
		return -1
	}
	block := fs.Superblock.FreeBlocks[0]
	fs.Superblock.FreeBlocks = fs.Superblock.FreeBlocks[1:]
	return block
}

// Initialize a directory inode
func initializeDir(inode *Inode) {
	btree := newBTree()
	inode.BlockPointer = allocateBlock()
	fs.DataBlocks[inode.BlockPointer] = serializeBTree(btree)
}

func newBTree() *BTree {
	return &BTree{
		Root: &BTreeNode{
			IsLeaf:   true,
			Keys:     make([]DirEntry, 0, MaxKeys),
			Children: make([]*BTreeNode, 0),
		},
	}
}

// insert adds a directory entry to the tree
func (t *BTree) insert(entry DirEntry) {
	root := t.Root
	if len(root.Keys) == MaxKeys {
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Children: []*BTreeNode{root},
		}
		root.Parent = newRoot
		t.splitChild(newRoot, 0)
		t.Root = newRoot
	}
	t.insertNonFull(t.Root, entry)
}

func (t *BTree) insertNonFull(node *BTreeNode, entry DirEntry) {
	i := len(node.Keys) - 1

	if node.IsLeaf {
		node.Keys = append(node.Keys, DirEntry{}) // Make space for the new entry
		for i >= 0 && entry.Name < node.Keys[i].Name {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = entry
	} else {
		for i >= 0 && entry.Name < node.Keys[i].Name {
			i--
		}
		i++
		if len(node.Children[i].Keys) == MaxKeys {
			t.splitChild(node, i)
			if entry.Name > node.Keys[i].Name {
				i++
			}
		}
		t.insertNonFull(node.Children[i], entry)
	}
}

func (t *BTree) splitChild(parent *BTreeNode, index int) {
	fullChild := parent.Children[index]
	newChild := &BTreeNode{
		IsLeaf:   fullChild.IsLeaf,
		Keys:     make([]DirEntry, MaxKeys/2),
		Children: make([]*BTreeNode, 0),
		Parent:   parent,
	}

	midIndex := MaxKeys / 2
	parent.Keys = append(parent.Keys[:index], append([]DirEntry{fullChild.Keys[midIndex]}, parent.Keys[index:]...)...)
	parent.Children = append(parent.Children[:index+1], append([]*BTreeNode{newChild}, parent.Children[index+1:]...)...)

	newChild.Keys = fullChild.Keys[midIndex+1:]
	fullChild.Keys = fullChild.Keys[:midIndex]

	if !fullChild.IsLeaf {
		newChild.Children = fullChild.Children[midIndex+1:]
		fullChild.Children = fullChild.Children[:midIndex+1]
		for _, child := range newChild.Children {
			child.Parent = newChild
		}
	}
}

func serializeBTree(btree *BTree) []byte {
	var data []byte
	serializeNode(btree.Root, &data)
	return data
}

func serializeNode(node *BTreeNode, data *[]byte) {
	// serialize the node keys
	for _, key := range node.Keys {
		*data = append(*data, []byte(fmt.Sprintf("%s:%d;", key.Name, key.InodeIndex))...)
	}
	*data = append(*data, '\n')

	// serialize the children
	if !node.IsLeaf {
		for _, child := range node.Children {
			serializeNode(child, data)
		}
	}
}

func deserializeBTree(data []byte) *BTree {
	btree := newBTree()
	nodeData := strings.Split(string(data), "\n")
	btree.Root = deserializeNode(nodeData, 0, nil)
	return btree
}

func deserializeNode(data []string, index int, parent *BTreeNode) *BTreeNode {
	node := &BTreeNode{
		Keys:     make([]DirEntry, 0),
		Children: make([]*BTreeNode, 0),
		Parent:   parent,
	}
	if index >= len(data) {
		return node
	}

	// Deserialize keys
	keyData := strings.Split(data[index], ";")
	for _, key := range keyData {
		if key == "" {
			continue
		}
		parts := strings.Split(key, ":")
		inodeIndex := atoi(parts[1])
		node.Keys = append(node.Keys, DirEntry{Name: parts[0], InodeIndex: inodeIndex})
	}

	// Deserialize children
	if len(data[index+1]) > 0 {
		for i := index + 1; i < len(data); i++ {
			if data[i] == "" {
				continue
			}
			child := deserializeNode(data, i, node)
			node.Children = append(node.Children, child)
		}
	}

	return node
}

func atoi(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// Journal functions
func addJournalEntry(operation, path string, data interface{}) {
	entry := JournalEntry{
		Operation: operation,
		Path:      path,
		Data:      data,
	}
	fs.Journal = append(fs.Journal, entry)
	if len(fs.Journal) > JournalMax {
		fs.Journal = fs.Journal[1:]
	}
}

func replayJournal() {
	for _, entry := range fs.Journal {
		switch entry.Operation {
		case "mkdir":
			data := entry.Data.(map[string]interface{})
			mkdirInternal(data["parentPath"].(string), data["dirName"].(string))
		case "touch":
			data := entry.Data.(map[string]interface{})
			touchInternal(data["dirPath"].(string), data["fileName"].(string))
		}
	}
}

// Directory operations
func mkdir(parentPath, dirName string) {
	addJournalEntry("mkdir", parentPath+"/"+dirName, map[string]interface{}{
		"parentPath": parentPath,
		"dirName":    dirName,
	})
	mkdirInternal(parentPath, dirName)
}

func mkdirInternal(parentPath, dirName string) {
	parentInode := resolvePath(parentPath)
	if parentInode == nil || !parentInode.IsDirectory {
		fmt.Println("Invalid parent directory")
		return
	}

	newDirInode := createInode(dirName, true, parentInode)
	fs.Superblock.InodeMap = append(fs.Superblock.InodeMap, newDirInode)

	entry := DirEntry{Name: dirName, InodeIndex: newDirInode.InodeNumber}
	addEntryToDir(parentInode, entry)
}

func touch(dirPath, fileName string) {
	addJournalEntry("touch", dirPath+"/"+fileName, map[string]interface{}{
		"dirPath":  dirPath,
		"fileName": fileName,
	})
	touchInternal(dirPath, fileName)
}

func touchInternal(dirPath, fileName string) {
	dirInode := resolvePath(dirPath)
	if dirInode == nil || !dirInode.IsDirectory {
		fmt.Println("Invalid directory")
		return
	}

	fileInode := createInode(fileName, false, dirInode)
	fs.Superblock.InodeMap = append(fs.Superblock.InodeMap, fileInode)

	entry := DirEntry{Name: fileName, InodeIndex: fileInode.InodeNumber}
	addEntryToDir(dirInode, entry)
}

func addEntryToDir(inode *Inode, entry DirEntry) {
	btree := deserializeBTree(fs.DataBlocks[inode.BlockPointer])
	btree.insert(entry)
	fs.DataBlocks[inode.BlockPointer] = serializeBTree(btree)
}

// Directory listing
func ls(path string) {
	inode := resolvePath(path)
	if inode == nil || !inode.IsDirectory {
		fmt.Println("Invalid directory")
		return
	}

	btree := deserializeBTree(fs.DataBlocks[inode.BlockPointer])
	listBTree(btree.Root)
}

func listBTree(node *BTreeNode) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.Keys); i++ {
		if !node.IsLeaf {
			listBTree(node.Children[i])
		}
		fmt.Println(node.Keys[i].Name)
	}
	if !node.IsLeaf {
		listBTree(node.Children[len(node.Children)-1])
	}
}

// Path resolution
func resolvePath(path string) *Inode {
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] != "root" {
		return nil
	}

	inode := fs.Superblock.InodeMap[0] // Start with the root inode
	for _, part := range parts[1:] {
		if part == "" {
			continue
		}

		found := false
		btree := deserializeBTree(fs.DataBlocks[inode.BlockPointer])
		for _, entry := range btree.Root.Keys {
			if entry.Name == part {
				inode = fs.Superblock.InodeMap[entry.InodeIndex]
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	return inode
}

// Consistency check function
func checkFilesystemConsistency() {
	usedBlocks := make(map[int]bool)
	usedInodes := make(map[int]bool)

	// Check inode consistency
	for _, inode := range fs.Superblock.InodeMap {
		if inode == nil {
			continue
		}
		if inode.InodeNumber < 0 || inode.InodeNumber >= fs.Superblock.TotalInodes {
			fmt.Printf("Invalid inode number: %d\n", inode.InodeNumber)
			return
		}
		if usedInodes[inode.InodeNumber] {
			fmt.Printf("Duplicate inode number: %d\n", inode.InodeNumber)
			return
		}
		usedInodes[inode.InodeNumber] = true

		// Check directory consistency
		if inode.IsDirectory {
			btree := deserializeBTree(fs.DataBlocks[inode.BlockPointer])
			if btree == nil {
				fmt.Printf("Invalid B-tree for directory inode: %d\n", inode.InodeNumber)
				return
			}
			checkBTreeConsistency(btree.Root, inode.InodeNumber)
		}

		// Check block consistency
		if inode.BlockPointer < 0 || inode.BlockPointer >= MaxBlocks {
			fmt.Printf("Invalid block pointer: %d\n", inode.BlockPointer)
			return
		}
		if usedBlocks[inode.BlockPointer] {
			fmt.Printf("Duplicate block pointer: %d\n", inode.BlockPointer)
			return
		}
		usedBlocks[inode.BlockPointer] = true
	}

	// Check free block consistency
	for _, block := range fs.Superblock.FreeBlocks {
		if usedBlocks[block] {
			fmt.Printf("Block marked as free but used: %d\n", block)
			return
		}
		usedBlocks[block] = true
	}

	fmt.Println("Filesystem consistency check passed")
}

// Check B-tree consistency
func checkBTreeConsistency(node *BTreeNode, parentInode int) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.Keys); i++ {
		entry := node.Keys[i]
		inode := fs.Superblock.InodeMap[entry.InodeIndex]
		if inode == nil {
			fmt.Printf("Invalid inode reference in B-tree: %d\n", entry.InodeIndex)
			return
		}
		if inode.Parent.InodeNumber != parentInode {
			fmt.Printf("Inode parent mismatch: %d\n", entry.InodeIndex)
			return
		}

		if !node.IsLeaf {
			checkBTreeConsistency(node.Children[i], parentInode)
		}
	}
	if !node.IsLeaf {
		checkBTreeConsistency(node.Children[len(node.Children)-1], parentInode)
	}
}

var filesystemSnapshots []Snapshot
var directorySnapshots map[string]DirectorySnapshot

func init() {
	directorySnapshots = make(map[string]DirectorySnapshot)
}

// Create a snapshot of the entire filesystem
func createFilesystemSnapshot() {
	snapshot := Snapshot{
		Inodes:     make([]*Inode, len(fs.Superblock.InodeMap)),
		DataBlocks: fs.DataBlocks,
	}

	copy(snapshot.Inodes, fs.Superblock.InodeMap)
	filesystemSnapshots = append(filesystemSnapshots, snapshot)
	fmt.Println("Filesystem snapshot created")
}

// Restore the latest filesystem snapshot
func restoreFilesystemSnapshot() {
	if len(filesystemSnapshots) == 0 {
		fmt.Println("No filesystem snapshots available")
		return
	}

	snapshot := filesystemSnapshots[len(filesystemSnapshots)-1]
	fs.Superblock.InodeMap = snapshot.Inodes
	fs.DataBlocks = snapshot.DataBlocks
	fmt.Println("Filesystem snapshot restored")
}

// createDirectorySnapshot creates a snapshot of a specific directory
func createDirectorySnapshot(path string) {
	inode := resolvePath(path)
	if inode == nil || !inode.IsDirectory {
		fmt.Println("Invalid directory")
		return
	}

	snapshot := DirectorySnapshot{
		RootInode:  inode,
		Inodes:     make([]*Inode, 0),
		DataBlocks: fs.DataBlocks,
	}

	snapshot.Inodes = append(snapshot.Inodes, inode)
	snapshotDirectory(inode, &snapshot)
	directorySnapshots[path] = snapshot
	fmt.Println("Directory snapshot created for:", path)
}

// snapshotDirectory stores directory records
func snapshotDirectory(inode *Inode, snapshot *DirectorySnapshot) {
	btree := deserializeBTree(fs.DataBlocks[inode.BlockPointer])
	for _, entry := range btree.Root.Keys {
		childInode := fs.Superblock.InodeMap[entry.InodeIndex]
		snapshot.Inodes = append(snapshot.Inodes, childInode)
		if childInode.IsDirectory {
			snapshotDirectory(childInode, snapshot)
		}
	}
}

// restoreDirectorySnapshot restores a specific directory snapshot
func restoreDirectorySnapshot(path string) {
	snapshot, exists := directorySnapshots[path]
	if !exists {
		fmt.Println("No snapshot available for directory:", path)
		return
	}

	for _, inode := range snapshot.Inodes {
		fs.Superblock.InodeMap[inode.InodeNumber] = inode
	}
	fs.DataBlocks = snapshot.DataBlocks
	fmt.Println("Directory snapshot restored for:", path)
}

func main() {
	initializeFS()

	// Replay the journal to recover from a crash
	replayJournal()

	// Create a new directory and file
	mkdir("/root", "dir1")
	touch("/root/dir1", "file1")

	// Create a filesystem snapshot
	createFilesystemSnapshot()

	// Modify the filesystem
	mkdir("/root", "dir2")
	touch("/root/dir2", "file2")

	ls("/root")

	// Restore the filesystem snapshot
	restoreFilesystemSnapshot()

	// List root directory after restoring snapshot
	ls("/root")

	// Create a directory snapshot
	createDirectorySnapshot("/root/dir1")

	// Modify the directory
	touch("/root/dir1", "file2")

	ls("/root/dir1")

	// Restore the directory snapshot
	restoreDirectorySnapshot("/root/dir1")

	// List directory after restoring snapshot
	ls("/root/dir1")

	checkFilesystemConsistency()
}
