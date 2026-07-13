package jobhandler

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"math"
)

type ImageResizeRequest struct {
	Data            []byte
	Width           int
	Height          int
	KeepAspectRatio bool
	OutputFormat    string
}

func (h *Handler) ResizeImage(ctx context.Context, req ImageResizeRequest) ([]byte, error) {
	_ = ctx

	if len(req.Data) == 0 {
		return nil, fmt.Errorf("jobhandler: image data is required")
	}
	if req.Width <= 0 || req.Height <= 0 {
		return nil, fmt.Errorf("jobhandler: width and height must be greater than zero")
	}

	src, format, err := image.Decode(bytes.NewReader(req.Data))
	if err != nil {
		return nil, err
	}

	bounds := src.Bounds()
	srcWidth := bounds.Dx()
	srcHeight := bounds.Dy()
	targetWidth := req.Width
	targetHeight := req.Height

	if req.KeepAspectRatio {
		widthScale := float64(targetWidth) / float64(srcWidth)
		heightScale := float64(targetHeight) / float64(srcHeight)
		scale := math.Min(widthScale, heightScale)
		if scale <= 0 {
			return nil, fmt.Errorf("jobhandler: invalid resize dimensions")
		}

		targetWidth = int(math.Max(1, math.Round(float64(srcWidth)*scale)))
		targetHeight = int(math.Max(1, math.Round(float64(srcHeight)*scale)))
	}

	resized := resizeNearest(src, targetWidth, targetHeight)
	outputFormat := req.OutputFormat
	if outputFormat == "" {
		outputFormat = format
	}

	return encodeImage(resized, outputFormat)
}

func resizeNearest(src image.Image, width, height int) *image.RGBA {
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	srcBounds := src.Bounds()
	srcWidth := srcBounds.Dx()
	srcHeight := srcBounds.Dy()

	for y := 0; y < height; y++ {
		srcY := srcBounds.Min.Y + int(float64(y)*float64(srcHeight)/float64(height))
		if srcY >= srcBounds.Max.Y {
			srcY = srcBounds.Max.Y - 1
		}
		for x := 0; x < width; x++ {
			srcX := srcBounds.Min.X + int(float64(x)*float64(srcWidth)/float64(width))
			if srcX >= srcBounds.Max.X {
				srcX = srcBounds.Max.X - 1
			}
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}

	return dst
}

func encodeImage(img image.Image, format string) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch normalizeFormat(format) {
	case "jpeg", "jpg":
		if err := jpegEncode(buf, img); err != nil {
			return nil, err
		}
	case "gif":
		if err := gifEncode(buf, img); err != nil {
			return nil, err
		}
	default:
		if err := pngEncode(buf, img); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
