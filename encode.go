package dca

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jogramming/ogg"
)

// AudioApplication is an application profile for opus encoding
type AudioApplication string

// ErrBadFrame
var ErrBadFrame = errors.New("Bad Frame")

// EncodeOptions is a set of options for encoding dca
type EncodeOptions struct {
	FrameRate        int  // audio sampling rate (ex 48000)
	FrameDuration    int  // audio frame duration can be 20, 40, or 60 (ms)
	Bitrate          int  // audio encoding bitrate in kb/s can be 8 - 128
	PacketLoss       int  // expected packet loss percentage
	CompressionLevel int  // Compression level, higher is better qualiy but slower encoding (0 - 10)
	BufferedFrames   int  // How big the frame buffer should be
	Threads          int  // Number of threads to use, 0 for auto
	VariableBitrate  bool // Whether vbr is used or not (variable bitrate)
	StartTime        int  // Start Time of the input stream in seconds
}

// StdEncodeOptions is the standard options for encoding
var StdEncodeOptions = &EncodeOptions{
	FrameRate:        48000,
	FrameDuration:    20,
	Bitrate:          64,
	CompressionLevel: 10,
	PacketLoss:       1,
	BufferedFrames:   100, // At 20ms frames that's 2s
	VariableBitrate:  true,
	StartTime:        0,
}

// EncodeStats is transcode stats reported by ffmpeg
type EncodeStats struct {
	Size     int
	Duration time.Duration
	Bitrate  float32
	Speed    float32
}

type Frame struct {
	data     []byte
	metaData bool
}

type EncodeSession struct {
	sync.Mutex
	options    *EncodeOptions
	pipeReader io.Reader
	filePath   string

	running      bool
	started      time.Time
	frameChannel chan *Frame
	process      *os.Process
	lastStats    *EncodeStats

	lastFrame int
	err       error

	ffmpegOutput string

	// buffer that stores unread bytes (not full frames)
	// used to implement io.Reader
	buf bytes.Buffer
}

// EncodeFile encodes the file/url/other in path
func EncodeFile(path string, options *EncodeOptions) (session *EncodeSession, err error) {
	session = &EncodeSession{
		options:      options,
		filePath:     path,
		frameChannel: make(chan *Frame, options.BufferedFrames),
	}
	go session.run()
	return
}

func (e *EncodeSession) run() {
	// Reset running state
	defer func() {
		e.Lock()
		e.running = false
		e.Unlock()
	}()

	e.Lock()
	e.running = true

	inFile := "pipe:0"
	if e.filePath != "" {
		inFile = e.filePath
	}

	if e.options == nil {
		e.options = StdEncodeOptions
	}

	args := []string{
		"-i", inFile,
		"-reconnect", "1",
		"-reconnect_at_eof", "1",
		"-reconnect_streamed", "1",
		"-reconnect_delay_max", "2",
		"-map", "0:a",
		"-acodec", "libopus",
		"-f", "ogg",
		"-compression_level", strconv.Itoa(e.options.CompressionLevel),
		"-ar", strconv.Itoa(e.options.FrameRate),
		"-ac", "2",
		"-b:a", strconv.Itoa(e.options.Bitrate * 1000),
		"-application", "audio",
		"-frame_duration", strconv.Itoa(e.options.FrameDuration),
		"-packet_loss", strconv.Itoa(e.options.PacketLoss),
		"-threads", strconv.Itoa(e.options.Threads),
		"-vf", "volume=0.5",
		//"pipe:1",
		"-ss", strconv.Itoa(e.options.StartTime),
	}

	args = append(args, "pipe:1")

	ffmpeg := exec.Command("ffmpeg", args...)

	// Print the ffmpeg command for debugging
	log.Printf("Executing command: %v", ffmpeg.Args)

	if e.pipeReader != nil {
		ffmpeg.Stdin = e.pipeReader
	}

	stdout, err := ffmpeg.StdoutPipe()
	if err != nil {
		e.Unlock()
		logln("StdoutPipe Error:", err)
		close(e.frameChannel)
		return
	}

	stderr, err := ffmpeg.StderrPipe()
	if err != nil {
		e.Unlock()
		logln("StderrPipe Error:", err)
		close(e.frameChannel)
		return
	}

	// Starts the ffmpeg command
	err = ffmpeg.Start()
	if err != nil {
		e.Unlock()
		logln("RunStart Error:", err)
		close(e.frameChannel)
		return
	}

	e.started = time.Now()

	e.process = ffmpeg.Process
	e.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)
	go e.readStderr(stderr, &wg)

	defer close(e.frameChannel)
	e.readStdout(stdout)
	wg.Wait()
	err = ffmpeg.Wait()
	if err != nil {
		if err.Error() != "signal: killed" {
			e.Lock()
			e.err = err
			e.Unlock()
		}
	}
}

func (e *EncodeSession) readStderr(stderr io.ReadCloser, wg *sync.WaitGroup) {
	defer wg.Done()

	bufReader := bufio.NewReader(stderr)
	var outBuf bytes.Buffer
	for {
		r, _, err := bufReader.ReadRune()
		if err != nil {
			if err != io.EOF {
				logln("Error Reading stderr:", err)
			}
			break
		}

		// Is this the best way to distinguish stats from messages?
		switch r {
		case '\r':
			// Stats line
			if outBuf.Len() > 0 {
				e.handleStderrLine(outBuf.String())
				outBuf.Reset()
			}
		case '\n':
			// Message
			e.Lock()
			e.ffmpegOutput += outBuf.String() + "\n"
			e.Unlock()
			outBuf.Reset()
		default:
			outBuf.WriteRune(r)
		}
	}
}

func (e *EncodeSession) handleStderrLine(line string) {
	if strings.Index(line, "size=") != 0 {
		return // Not stats info
	}

	var size int

	var timeH int
	var timeM int
	var timeS float32

	var bitrate float32
	var speed float32

	_, err := fmt.Sscanf(line, "size=%dkB time=%d:%d:%f bitrate=%fkbits/s speed=%fx", &size, &timeH, &timeM, &timeS, &bitrate, &speed)
	if err != nil {
		logln("Error parsing ffmpeg stats:", err)
	}

	dur := time.Duration(timeH) * time.Hour
	dur += time.Duration(timeM) * time.Minute
	dur += time.Duration(timeS) * time.Second

	stats := &EncodeStats{
		Size:     size,
		Duration: dur,
		Bitrate:  bitrate,
		Speed:    speed,
	}

	e.Lock()
	e.lastStats = stats
	e.Unlock()
}

func (e *EncodeSession) readStdout(stdout io.ReadCloser) {
	decoder := ogg.NewPacketDecoder(ogg.NewDecoder(stdout))

	// the first 2 packets are ogg opus metadata
	skipPackets := 2
	for {
		// Retrieve a packet
		packet, _, err := decoder.Decode()
		if skipPackets > 0 {
			skipPackets--
			continue
		}
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading ffmpeg stdout:", err)
			}
			break
		}

		err = e.writeOpusFrame(packet)
		if err != nil {
			log.Println("Error writing opus frame:", err)
			break
		}
	}
}

func (e *EncodeSession) writeOpusFrame(opusFrame []byte) error {
	var dcaBuf bytes.Buffer

	err := binary.Write(&dcaBuf, binary.LittleEndian, int16(len(opusFrame)))
	if err != nil {
		return err
	}

	_, err = dcaBuf.Write(opusFrame)
	if err != nil {
		return err
	}

	e.frameChannel <- &Frame{dcaBuf.Bytes(), false}

	e.Lock()
	e.lastFrame++
	e.Unlock()

	return nil
}

// Stop stops the encoding session
func (e *EncodeSession) Stop() error {
	e.Lock()
	defer e.Unlock()
	return e.process.Kill()
}

// ReadFrame blocks until a frame is read or there are no more frames
// Note: If rawoutput is not set, the first frame will be a metadata frame
func (e *EncodeSession) ReadFrame() (frame []byte, err error) {
	f := <-e.frameChannel
	if f == nil {
		return nil, io.EOF
	}

	return f.data, nil
}

// OpusFrame implements OpusReader, returning the next opus frame
func (e *EncodeSession) OpusFrame() (frame []byte, err error) {
	f := <-e.frameChannel
	if f == nil {
		return nil, io.EOF
	}

	if f.metaData {
		// Return the next one then...
		return e.OpusFrame()
	}

	if len(f.data) < 2 {
		return nil, ErrBadFrame
	}

	return f.data[2:], nil
}

// Running returns true if running
func (e *EncodeSession) Running() (running bool) {
	e.Lock()
	running = e.running
	e.Unlock()
	return
}

// Stats returns ffmpeg stats, NOTE: this is not playback stats but transcode stats.
// To get how far into playback you are
// you have to track the number of frames sent to discord youself
func (e *EncodeSession) Stats() *EncodeStats {
	s := &EncodeStats{}
	e.Lock()
	if e.lastStats != nil {
		*s = *e.lastStats
	}
	e.Unlock()

	return s
}

// Options returns the options used
func (e *EncodeSession) Options() *EncodeOptions {
	return e.options
}

// Truncate is deprecated, use Cleanup instead
// this will be removed in a future version
func (e *EncodeSession) Truncate() {
	e.Cleanup()
}

// Cleanup cleans up the encoding session, throwring away all unread frames and stopping ffmpeg
// ensuring that no ffmpeg processes starts piling up on your system
// You should always call this after it's done
func (e *EncodeSession) Cleanup() {
	e.Stop()

	for range e.frameChannel {
		// empty till closed
	}
}

// Read implements io.Reader,
// n == len(p) if err == nil, otherwise n contains the number bytes read before an error occured
func (e *EncodeSession) Read(p []byte) (n int, err error) {
	if e.buf.Len() >= len(p) {
		return e.buf.Read(p)
	}

	for e.buf.Len() < len(p) {
		f, err := e.ReadFrame()
		if err != nil {
			break
		}
		e.buf.Write(f)
	}

	return e.buf.Read(p)
}

// FrameDuration implements OpusReader, retruning the duration of each frame
func (e *EncodeSession) FrameDuration() time.Duration {
	return time.Duration(e.options.FrameDuration) * time.Millisecond
}

// Error returns any error that occured during the encoding process
func (e *EncodeSession) Error() error {
	e.Lock()
	defer e.Unlock()
	return e.err
}

// FFMPEGMessages returns messages printed by ffmpeg to stderr, you can use this to see what ffmpeg is saying if your encoding fails
func (e *EncodeSession) FFMPEGMessages() string {
	e.Lock()
	output := e.ffmpegOutput
	e.Unlock()
	return output
}
